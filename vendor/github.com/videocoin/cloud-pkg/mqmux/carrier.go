package mqmux

type RMQHeaderCarrier map[string]interface{}

// ForeachKey conforms to the TextMapReader interface.
func (c RMQHeaderCarrier) ForeachKey(handler func(key, val string) error) error {
	for k, v := range c {
		if err := handler(k, v.(string)); err != nil {
			return err
		}
	}
	return nil
}

// Set implements Set() of opentracing.TextMapWriter
func (c RMQHeaderCarrier) Set(key string, val string) {
	c[key] = val
}
