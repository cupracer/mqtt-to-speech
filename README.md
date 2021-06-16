# mqtt-to-speech

A roughly written MQTT client which receives a text message and forwards it to 
Amazon Polly to convert it to speech if not cached already.

The output is then cached in Redis, written to disk and played aftwards.
