# mqtt-to-speech

A roughly written MQTT client which receives a text message and forwards it to 
Amazon Polly to convert it to speech if not cached already.

The output is then cached in Redis, written to disk and played aftwards.

### Requirements

- Amazon AWS credentials with permission to use Amazon Polly
- MQTT message broker
- Redis (optional)

### Usage

Import class or run as stand-alone application.

- prepare mqtt server + topic
- optionally configure Redis for caching
- configure required variables (see env.dist and `main()` function for details)
- start program (stand-alone) or execute method `start_runner` (class usage)
- send MQTT message with `text` attribute

### MQTT example message
```json
{
  "text": "Hello World!"
}
```
