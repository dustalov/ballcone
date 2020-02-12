@0x9642e044fc0bd445;

annotation nginx @0xe6655f1b578e89e3 (field): Text;

struct Record @0x818f6074dad4858c {
  args @0 :Text $nginx("args");
  body @1 :UInt64 $nginx("body_bytes_sent");
  contentType @2 :Text $nginx("content_type");
  contentLength @3 :UInt64 $nginx("content_length");
  host @4 :Text $nginx("host");
  referrer @5 :Text $nginx("http_referrer");
  userAgent @6 :Text $nginx("http_user_agent");
  xForwardedFor @7 :Text $nginx("http_x_forwarded_for");
  remote @8 :Text $nginx("remote_addr");
  method @9 :Text $nginx("request_method");
  time @10 :Float32 $nginx("request_time");
  status @11 :UInt16 $nginx("status");
  upstream @12 :Text $nginx("upstream_addr");
  uri @13 :Text $nginx("uri");
}
