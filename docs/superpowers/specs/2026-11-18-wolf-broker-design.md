# Wolf — RabbitMQ uyumlu, düşük footprint'li mesaj broker'ı

Tarih: 2026-11-18
Durum: Taslak (onay bekliyor)

## 1. Amaç ve neden

Wolf, Rust ile yazılmış, RabbitMQ'nun yerine **kod değişikliği olmadan** konabilen bir
mesaj broker'ıdır. Mevcut Java ve Go RabbitMQ client'ları Wolf'a bağlanıp aynı
şekilde çalışmalıdır.

Var olma sebebi **performans ve düşük footprint**: aynı donanımda RabbitMQ'dan
daha yüksek throughput, daha düşük latency, çok daha az bellek ve Erlang VM'siz
tek bir statik binary.

Wolf aynı zamanda **Cellvoy showcase projesidir**: tasarımdan merge'e kadar
geliştirmenin büyük kısmı Cellvoy agent'ları tarafından yapılacak. Bu yüzden
iş, bağımsız ve paralel yürütülebilecek ticket'lara bölünebilir şekilde
modüler tutulur (bkz. §9).

## 2. Hedefler

1. **Client uyumluluğu** — Bu client'lar kodda değişiklik olmadan çalışır:
   - Java: `com.rabbitmq:amqp-client` (5.x), Spring AMQP
   - Go: `github.com/rabbitmq/amqp091-go`
   - Stream client'ları: `com.rabbitmq:stream-client` (Java),
     `github.com/rabbitmq/rabbitmq-stream-go-client` (Go)
2. **Özellik kapsamı (v1)** — bkz. §5.
3. **Performans** — Tek node'da **1.000.000 msg/s**, aşağıdaki koşullarda:
   - Mesaj boyutu 1 KB
   - Batch'li publish (publisher confirms açık, confirm'ler toplu döner)
   - Hedef: bir stream ya da tek bir klasik kuyruk
   - Dayanıklılık: group commit; çökme anında son birkaç ms'lik veri kaybı kabul
     edilebilir
   - Donanım varsayımı: NVMe disk, ≥ 25 GbE ağ (1 KB × 1M/s ≈ 1 GB/s yalnızca
     giriş trafiği; 10 GbE yetmez)
4. **Footprint** *(hedef değerler benim önerim, düzeltilebilir)*:
   - Boşta RSS < 30 MB
   - Açılış süresi < 1 sn (recovery hariç)
   - Tek statik binary (Linux'ta musl), container image < 30 MB
5. **Ölçülebilirlik** — RabbitMQ ile aynı donanımda yan yana karşılaştırma yapan,
   tekrarlanabilir bir benchmark düzeneği v1'in parçasıdır.
6. **Kubernetes'e hazır** (ama v1'de k8s'e özel bir şey yok) — bkz. §7.

## 3. Hedef olmayanlar (v1)

- Management UI ve tam Management HTTP API (yerine `/metrics` + health endpoint'leri)
- Clustering, replikasyon, quorum queue'lar
- Priority queue'lar, lazy queue modu, federation, shovel
- MQTT, STOMP, AMQP 1.0
- `headers` exchange, exchange-to-exchange binding *(benim önerim: sonraya)*
- LDAP/OAuth gibi harici kimlik doğrulama

## 4. Anti-hedefler (çalışsa bile başarısızlık sayılacaklar)

- RabbitMQ'dan **daha fazla** bellek kullanmak ya da boşta yüksek RSS
- "Destekleniyor" denen bir özelliğin RabbitMQ'dan farklı davranması (yarım
  uyumluluk, hiç desteklememekten kötüdür — client tarafında sessiz hatalara yol açar)
- Ölçülmemiş performans iddiaları
- Hot path'te mesaj başına heap allocation / byte kopyalama

## 5. Protokol ve özellik kapsamı (v1)

### 5.1 AMQP 0-9-1 (port 5672)

RabbitMQ'nun AMQP 0-9-1 lehçesi, RabbitMQ eklentileri dahil:

- Connection: handshake, `PLAIN` / `AMQPLAIN` auth, tune (frame_max, channel_max,
  heartbeat), heartbeat'ler, `connection.blocked` / `unblocked`, server
  capabilities tablosu (client'lar bunu kontrol eder: `publisher_confirms`,
  `basic.nack`, `consumer_cancel_notify`, `connection.blocked`,
  `per_consumer_qos`, `authentication_failure_close`)
- Channel: open/close/flow
- Exchange: `declare` (passive dahil), `delete`; tipler: **direct, fanout, topic**;
  default exchange (`""`) ve `amq.*` ön tanımlı exchange'ler
- Queue: `declare` (passive, durable, exclusive, auto-delete, server-named),
  `bind`, `unbind`, `purge`, `delete`
- Basic: `publish`, `consume`, `cancel`, `get`, `ack`, `nack` (multiple dahil),
  `reject`, `recover`, `qos` (prefetch; RabbitMQ'nun per-consumer semantiği),
  `return` (mandatory)
- Confirm: `confirm.select`, toplu `ack`/`nack`
- Tx: **v1'de yok** *(benim önerim; nadiren kullanılır ve yavaştır)*

Kuyruk argümanları (`x-*`):

| Argüman | Not |
|---|---|
| `x-queue-type` | `classic` (varsayılan) ve `stream` |
| `x-message-ttl` | Kuyruk seviyesinde TTL |
| `x-expires` | Kullanılmayan kuyruğun silinmesi |
| `x-dead-letter-exchange`, `x-dead-letter-routing-key` | DLX |
| `x-max-length`, `x-max-length-bytes`, `x-overflow` (`drop-head`, `reject-publish`) | Klasik kuyruklar |
| `x-max-age`, `x-max-length-bytes`, `x-stream-max-segment-size-bytes` | Stream retention |

Mesaj başına TTL: `expiration` property'si.

**TTL semantiği** RabbitMQ ile aynı olmalı: mesaj başına TTL'de süresi dolan
mesajlar yalnızca kuyruğun başına geldiklerinde düşürülür / dead-letter edilir.

**DLX semantiği:** sebepler `rejected` (requeue=false), `expired`, `maxlen`;
`x-death` header'ı RabbitMQ formatında (count, reason, queue, exchange,
routing-keys, time) ve `x-first-death-*` header'ları eklenir. Döngüsel
dead-lettering RabbitMQ'daki gibi tespit edilip kesilir.

**Stream'lere AMQP ile erişim:** RabbitMQ kuralları geçerlidir: consumer'lar
manuel ack ve prefetch kullanmak zorundadır, `x-stream-offset` argümanı
(`first`, `last`, `next`, sayısal offset, timestamp) desteklenir.

### 5.2 RabbitMQ Stream protokolü (port 5552) *(benim önerim — onay gerekiyor)*

Native stream client'ları bu binary protokolü kullanır ve 1M msg/s hedefine
gerçekçi olarak bu yoldan ulaşılır. v1 kapsamı:

- `PeerProperties`, `SaslHandshake`, `SaslAuthenticate`, `Tune`, `Open`, `Close`,
  `Heartbeat`
- `Create`, `Delete`, `Metadata`, `QueryPublisherSequence`
- `DeclarePublisher`, `Publish` (deduplication için publisher id + sequence),
  `PublishConfirm`, `PublishError`, `DeletePublisher`
- `Subscribe` (offset spec'leri), `Deliver`, `Credit`, `Unsubscribe`
- `StoreOffset`, `QueryOffset` (server-side offset tracking)
- `StreamStats`, `ExchangeCommandVersions`

Super stream'ler, single active consumer ve filtering v1'de yok.

Aynı stream'e AMQP ve stream protokolü ile aynı anda yazılıp okunabilir; ikisi
aynı log'u paylaşır.

### 5.3 Güvenlik ve çoklu kiracılık

- Kullanıcılar ve vhost'lar config dosyasından tanımlanır (v1'de runtime'da
  yönetim API'si yok).
- Basit izin modeli: kullanıcı başına vhost erişimi *(configure/write/read regex'leri
  sonraya)*.
- RabbitMQ'daki gibi `guest/guest` varsayılan kullanıcı yalnızca localhost'tan
  bağlanabilir.
- TLS (AMQPS 5671, stream TLS 5551): `rustls` ile, v1 kapsamında *(benim önerim)*.

### 5.4 Gözlemlenebilirlik (port 15692, HTTP)

- `/metrics` — Prometheus formatı (RabbitMQ'nun Prometheus plugin'i ile aynı port;
  metrik isimlerinin mümkün olduğunca `rabbitmq_*` ile uyumlu olması dashboard'ların
  yeniden kullanımını kolaylaştırır)
- `/healthz` (liveness), `/readyz` (recovery bitti, listener'lar açık)

## 6. Mimari

**Seçilen yaklaşım:** Tokio multi-thread runtime, kuyruk başına bir task ve ortak,
log tabanlı storage.

```
           ┌───────────────┐   ┌─────────────────┐
 TCP 5672 →│ AMQP conn task│   │ Stream conn task│← TCP 5552
           │ codec+channel │   │ codec           │
           └──────┬────────┘   └───────┬─────────┘
                  │ publish (Bytes)    │
                  ▼                    │
           ┌───────────────┐           │
           │ Router        │ vhost → exchange → binding (topic trie)
           └──────┬────────┘           │
                  │ bounded mpsc       │
        ┌─────────┴─────────┐          │
        ▼                   ▼          ▼
  ┌───────────┐       ┌───────────┐
  │Queue task │  ...  │Stream task│
  │(classic)  │       │           │
  └─────┬─────┘       └─────┬─────┘
        ▼                   ▼
  ┌──────────────────────────────────┐
  │ Storage: segmentli append-only log│  (trait arkasında)
  └──────────────────────────────────┘
```

### 6.1 Bileşenler (crate'ler)

| Crate | Sorumluluk |
|---|---|
| `wolf-amqp-codec` | AMQP 0-9-1 frame ve method encode/decode, field table'lar. Saf, I/O yok. |
| `wolf-stream-codec` | Stream protokolü frame encode/decode. Saf, I/O yok. |
| `wolf-storage` | Segmentli log, index, recovery, retention. `Storage` trait'i ve dosya tabanlı implementasyon. |
| `wolf-broker` | Domain: vhost, exchange, binding, router (topic trie), queue/stream task'ları, TTL, DLX, metadata store. Protokolden bağımsız. |
| `wolf-server` | Binary: config, listener'lar, connection/channel state machine'leri (codec ↔ broker), HTTP (metrics/health), sinyal yönetimi. |
| `wolf-bench` *(sonra)* | Benchmark senaryoları ve RabbitMQ karşılaştırma script'leri. |

Kuralı: codec crate'leri ve `wolf-storage`, `wolf-broker`'a bağımlı değildir.
`wolf-broker` herhangi bir wire protokolünü bilmez.

### 6.2 Publish yolu

1. Connection task'ı frame'leri okur ve method + header + body'yi birleştirir.
   Body `bytes::Bytes` olarak tutulur, kopyalanmaz.
2. Router, exchange ve binding'leri çözer. Topic eşleşmesi trie üzerinden yapılır,
   sonuç (exchange, routing key) bazında cache'lenebilir.
3. Mesaj hedef kuyrukların her birine **aynı `Bytes`** referansıyla gönderilir
   (fanout'ta kopya yok).
4. Kuyruk task'ları arasındaki kanallar sınırlıdır. Kanallar dolduğunda
   connection'ın okuması yavaşlar (TCP backpressure). Toplam bellek eşiği
   aşıldığında `connection.blocked` gönderilir (RabbitMQ memory alarm
   semantiği).
5. Kuyruk ya da stream mesajı log'a ekler.
6. **Group commit:** yazmalar biriktirilip `flush_interval` (varsayılan birkaç ms)
   ya da `flush_bytes` dolduğunda topluca yazılır. Publisher confirm'leri
   yazma işletim sistemine teslim edildikten sonra döner. `fsync` periyodiktir
   ve süresi config'den ayarlanır. Bu, "ms mertebesinde kayıp kabul edilebilir"
   kararının uygulamasıdır.

### 6.3 Consume yolu

- Klasik kuyruk task'ı, consumer'lara prefetch limitine göre round-robin dağıtır.
- Yeni mesajlar bellekteki tail cache'ten, geride kalan consumer'lar segment
  dosyasından okunur.
- Stream consumer'ları kendi offset'leriyle bağımsız okur. Stream protokolünde
  chunk'lar diskten olduğu gibi gönderilebilir (ileride `sendfile` ile
  zero-copy için uygun bir format).

### 6.4 Storage

- Her kuyruk ve stream için bir dizin bulunur ve içinde sıralı segment dosyaları yer alır.
- Kayıtlar batch/chunk halinde yazılır: header (offset, timestamp, kayıt sayısı,
  CRC32C) ve ardından mesajlar. **Chunk formatını RabbitMQ stream chunk
  formatına yakın tutmak** stream protokolünde chunk'ı dönüştürmeden göndermeyi
  sağlar *(builder'a bırakılmış detay; hedef zero-copy deliver)*.
- Seyrek offset → dosya pozisyonu index'i ve timestamp index'i tutulur.
- **Klasik kuyruklar log üstünde:** ack edilen offset'ler bir ack yapısında
  (bitmap ya da aralık listesi) tutulur. Bir segmentteki tüm mesajlar ack edilince
  ya da expire olunca segment silinir. Ack durumu periyodik olarak checkpoint edilir.
  Çökmede checkpoint sonrası ack'ler kaybolabilir ve bu mesajlar redelivered=true ile
  yeniden gönderilir (at-least-once, RabbitMQ ile uyumlu).
- **Recovery:** açılışta son segment taranır, CRC'si tutmayan yarım yazılmış kuyruk
  kesilir (truncate).
- **Retention (stream):** `x-max-age` ve `x-max-length-bytes`, segment seviyesinde
  uygulanır.
- `Storage` bir trait'tir. İleride io_uring tabanlı bir backend bu trait'in
  arkasına eklenebilir.

### 6.5 Metadata

Durable exchange, kuyruk, binding ve stream tanımları ile stream consumer
offset'leri, data dizininde küçük bir metadata dosyasında (append-only log ve
periyodik snapshot) saklanır. Harici bir veritabanı kullanılmaz.

## 7. Dağıtım ve işletim

- Tek binary: `wolf --config wolf.toml`. Config TOML dosyasından okunur ve
  `WOLF_*` env değişkenleriyle override edilir (k8s ConfigMap ve Secret ile uyumlu).
- Tüm kalıcı durum tek bir `data_dir` altındadır (k8s'te PersistentVolume).
- `SIGTERM`: yeni bağlantıları reddet, flush + fsync yap, bağlantıları
  `connection.close` ile kapat, çık. Kapanma süresi config'den ayarlanabilir.
- Loglar stdout'a gider (JSON ya da insan okunur format, config'den seçilir).
- Linux (x86_64 + aarch64, musl) birinci sınıf platformdur. macOS geliştirme
  platformudur ve testler orada da geçmelidir.

## 8. Doğrulama

1. **Birim testleri:** Codec'ler için round-trip ve RabbitMQ'dan alınmış örnek
   frame'ler; topic eşleşmesi (`*`, `#` kenar durumları); storage recovery
   (yarım yazılmış segment).
2. **Uyumluluk testleri:** Gerçek Java ve Go client'larıyla senaryolar çalıştırılır
   (declare, publish, confirm, consume, ack/nack, TTL'li mesajın DLX'e düşmesi,
   stream offset'leri). Aynı test paketi **hem RabbitMQ'ya hem Wolf'a** karşı
   koşulur, sonuçlar aynı olmalıdır. Bu test paketi drop-in iddiasının kanıtıdır.
3. **Performans:** Resmi RabbitMQ araçları olan `rabbitmq-perf-test` (AMQP) ve
   `stream-perf-test` (stream) değiştirilmeden kullanılır. Bu araçlar da bir
   uyumluluk testi işlevi görür. Throughput, p50/p99 latency ve RSS ölçülür.
   RabbitMQ (aynı sürüm, aynı donanım) ile karşılaştırmalı bir rapor üretilir.

## 9. Teslim sırası (Cellvoy için kilometre taşları)

Her kilometre taşının sonunda çalışan ve test edilebilir bir sistem olur:

1. **M0 — Bootstrap:** workspace iskeleti, lint/format/test komutları *(bu tasarımla birlikte yapılır)*
2. **M1 — AMQP çekirdeği:** codec, connection/channel, direct/fanout/topic, bellek içi
   kuyruklar, publish/consume/ack, confirm. Java ve Go client'larıyla hello-world.
3. **M2 — Kalıcılık:** segmentli log, durable kuyruklar, metadata, recovery.
4. **M3 — TTL, DLX, max-length.**
5. **M4 — Stream'ler:** AMQP üzerinden stream, ardından stream protokolü (5552).
6. **M5 — Performans:** benchmark düzeneği, profil ve optimizasyon, 1M msg/s hedefi.
7. **M6 — İşletim:** TLS, metrics/health, graceful shutdown, container image.

`wolf-amqp-codec`, `wolf-stream-codec` ve `wolf-storage` birbirinden bağımsızdır,
bu yüzden paralel ticket'lar olarak yürütülebilirler.

### 9.1 İlk teslim: M1 — "basic RabbitMQ klonu" (ilk Cellvoy epic'i)

Jira: epic `WOLF` projesinde (byteflow.atlassian.net) açılır. Epic yalnızca bu
bölümü kapsar. M2–M6 bağlam içindir, bu epic'e ticket olarak girmez.

M1 tek başına teslim edilebilir bir üründür: gerçek RabbitMQ client'larının
bağlanabildiği, mesajları bellekte tutan bir broker. Kalıcılık, TTL, DLX ve
stream'ler bu teslimde **yoktur**.

Kapsam:

- `wolf` binary'si 5672'de dinler. Varsayılan `guest/guest` kullanıcısı ve `/` vhost'u vardır.
- Connection: handshake, `PLAIN` auth, tune, heartbeat, close. Channel: open/close.
- Exchange: direct, fanout ve topic; default exchange ve `amq.*` exchange'leri;
  declare (passive dahil) ve delete.
- Queue: declare (server-named, exclusive, auto-delete dahil), bind, unbind,
  purge, delete. `durable` bayrağı kabul edilir ama kuyruk bellekte tutulur.
- Basic: publish (mandatory → `basic.return`), consume, cancel, get, ack, nack,
  reject, qos (prefetch), recover. Bağlantı koptuğunda ack edilmemiş mesajlar
  requeue edilir.
- Publisher confirms (`confirm.select`).
- Desteklenmeyen bir özellik (ör. `x-queue-type=stream`, `tx.select`) istendiğinde
  RabbitMQ'nun kullandığı reply code'larla düzgün bir hata döner. Sessizce yok
  sayılmaz.

Kabul kriterleri: agent'ların doğrulayacakları (CI/devcontainer, yalnızca Rust):

1. `.loopflow.yml` validation adımları (fmt, clippy, test) geçer.
2. Uçtan uca testler gerçek bir AMQP client'ı olan `lapin` ile yazılır (dev-dependency).
   Testler Wolf'u process içinde, rastgele bir portta başlatır. Kapsanan senaryolar:
   topic routing (`*` ve `#`), fanout, direct, prefetch ile round-robin dağıtım,
   ack, nack/requeue (`redelivered` bayrağı dahil), reject, bağlantı kopunca
   requeue, publisher confirms, mandatory/return, `basic.get`, passive declare
   hataları ve desteklenmeyen özelliklerin doğru reply code ile reddedilmesi.

İnsan doğrulaması (ekip yapar, epic'in kapanmasını engellemez):

3. Java `amqp-client` ve Go `amqp091-go` ile yazılmış örnek producer/consumer'lar
   kodda değişiklik olmadan çalışır. Aynı senaryolar gerçek RabbitMQ'ya karşı da
   koşulur ve aynı sonucu verir.
4. `rabbitmq-perf-test` değiştirilmeden Wolf'a karşı çalışır. Bu teslimde
   throughput hedefi yok, sadece sonuç kaydedilir.

Not: Varsayılan `guest/guest` kullanıcısı RabbitMQ uyumluluğu için bilinçli
olarak var ve RabbitMQ'daki gibi yalnızca loopback'ten (`127.0.0.0/8`, `::1`)
bağlanabilir. Kimlik bilgileri artık `ServerConfig` üzerinden dışarıdan
verilebilir; `WOLF_DEFAULT_USER` ve `WOLF_DEFAULT_PASS` ortam değişkenleri
ikisi birlikte ayarlandığında varsayılan hesabın yerine geçer. İkisinden
yalnızca biri ayarlıysa sunucu net bir hata ile başlamayı reddeder. Env ile
tanımlanan `guest` dışı bir kullanıcı loopback kısıtına tabi değildir; parola
karşılaştırması tek yerde (`auth.rs`) kalır ve parolalar asla loglanmaz.

Semgrep `loopflow-rust-hardcoded-secret-or-insecure-tls` kuralı, varsayılan
guest parolasının env yokken kaynakta kalması nedeniyle yalnızca o tek satırda
(WOLF-22), gerekçeli bir `nosemgrep` yorumuyla bastırılır. Bu istisna yalnızca
loopback ile sınırlandırılmış, RabbitMQ uyumluluğu için tutulan varsayılan
hesap içindir; değişken yeniden adlandırılmaz, değer bölünmez ve dosya/blok
seviyesinde bastırma yapılmaz.

## 10. Değerlendirilen yaklaşımlar

1. **Tokio, kuyruk başına task ve ortak log storage — SEÇİLDİ.** macOS'ta
   geliştirilebilir, Linux ve k8s'te sorunsuz, ekosistemi olgun. Tek storage
   motoru sayesinde kod küçük kalır. 1 KB mesajda darboğaz runtime değil,
   batching ve kopyalamamaktır.
2. **Thread-per-core (monoio/glommio ve io_uring):** En yüksek tavan performansı
   verir. Ancak Linux'a bağımlıdır, macOS'ta geliştirmeyi zorlaştırır ve
   çekirdekler arası routing karmaşıktır. Storage trait'i sayesinde io_uring
   backend'i için kapı açık bırakıldı.
3. **Klasik kuyruklar ve stream'ler için ayrı storage:** Kısa ömürlü
   mesajlarda biraz daha hızlı olabilir, ama iki motor demektir ve
   footprint hedefine ters düşer.

## 11. Kullanıcının kararları

- Neden: performans ve düşük footprint
- Client'lar: Java ve Go
- Özellikler: topic exchange, DLX, TTL, stream'ler
- Performans: 1M msg/s, 1 KB mesaj, batch'li publish (senaryo A), ms
  mertebesinde kayıp kabul edilebilir
- Mimari: yaklaşım 1
- Dağıtım: şimdilik tek binary, k8s'e engel olmayacak
- Geliştirme Cellvoy ile yapılacak, Wolf Cellvoy için bir showcase

## 12. Benim kararlarım (onayınıza açık)

- Stream protokolü (5552) v1 kapsamında
- Footprint hedef rakamları (§2.4)
- Tx, headers exchange ve exchange-to-exchange binding v1'de yok
- TLS v1'de var (`rustls`)
- Kullanıcı ve vhost tanımları yalnızca config dosyasından
- Confirm'ler OS'e yazmadan sonra dönüyor, fsync periyodik
- Crate ayrımı (§6.1) ve kilometre taşı sırası (§9)

## 13. Builder'a bırakılanlar

- Chunk ve segment dosya formatının ayrıntıları, index yapısı
- Ack takibi için veri yapısı (bitmap, aralık listesi vb.)
- Topic trie implementasyonu ve route cache politikası
- Config şemasının ayrıntıları ve varsayılan değerler
- Kullanılacak crate'ler (`tokio`, `bytes` ve `rustls` dışında)
- Uyumluluk test paketinin dili ve düzeni (Java/Go test projeleri, docker-compose vb.)
