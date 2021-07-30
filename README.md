This is an example of a simple kafka http proxy that can do sync or async produce requests.

Start up the kafka broker and zookeper locally.

./gradlew :run

curl -d "some data" -X POST http://localhost:8090/example/produce\?call-mode\wait-for-ack

