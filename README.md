# Foxglove WebSocket publishing in Rust

This library provides means to publish messages to the amazing Foxglove UI in
Rust. It implements part of the Foxglove WebSocket protocol described in
https://github.com/foxglove/ws-protocol.

## Example

Call

```
cargo run --release --example string
```

to start an example application that publishes ROS 1 string data -- both
latching and non-latching.
