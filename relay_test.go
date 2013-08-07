package relay

import (
	"os"
	"sync"
	"testing"
	"time"
)

func CheckInteg(t *testing.T) {
	if os.Getenv("INTEG_TESTS") != "true" || AMQPHost() == "" {
		t.SkipNow()
	}
}

func AMQPHost() string {
	return os.Getenv("AMQP_HOST")
}

func testSendRecv(t *testing.T, r *Relay) {
	// Get a publisher
	pub, err := r.Publisher("test")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer pub.Close()

	// Get a consumer
	cons, err := r.Consumer("test")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer cons.Close()

	// Send a message
	msg := "the quick brown fox jumps over the lazy dog"
	err = pub.Publish(msg)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Try to get the message
	var in string
	err = cons.Consume(&in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Ack the message
	err = cons.Ack()
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Check message
	if in != msg {
		t.Fatalf("unexpected msg! %v %v", in, msg)
	}
}

func TestSimplePublishConsume(t *testing.T) {
	CheckInteg(t)

	conf := Config{Addr: AMQPHost()}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	testSendRecv(t, r)
}

func TestPublishNoConfirm(t *testing.T) {
	CheckInteg(t)

	conf := Config{Addr: AMQPHost(), DisablePublishConfirm: true}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	testSendRecv(t, r)
}

func TestPublishNoPersist(t *testing.T) {
	CheckInteg(t)

	conf := Config{Addr: AMQPHost(), DisablePersistence: true}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	testSendRecv(t, r)
}

func TestCustomExchange(t *testing.T) {
	CheckInteg(t)

	conf := Config{Addr: AMQPHost(), Exchange: "my-exchange"}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	testSendRecv(t, r)
}

func TestRelayMultiClose(t *testing.T) {
	CheckInteg(t)

	conf := Config{Addr: AMQPHost()}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("unexpected err")
	}
	if err := r.Close(); err != nil {
		t.Fatalf("unexpected err")
	}
}

func TestConsumerMultiClose(t *testing.T) {
	CheckInteg(t)

	conf := Config{Addr: AMQPHost()}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	cons, err := r.Consumer("test")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	if err := cons.Close(); err != nil {
		t.Fatalf("unexpected err")
	}
	if err := cons.Close(); err != nil {
		t.Fatalf("unexpected err")
	}
}

func TestPublisherMultiClose(t *testing.T) {
	CheckInteg(t)

	conf := Config{Addr: AMQPHost()}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	pub, err := r.Publisher("test")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	if err := pub.Close(); err != nil {
		t.Fatalf("unexpected err")
	}
	if err := pub.Close(); err != nil {
		t.Fatalf("unexpected err")
	}
}

func TestMultiConsume(t *testing.T) {
	CheckInteg(t)

	conf := Config{Addr: AMQPHost(), PrefetchCount: 5, EnableMultiAck: true}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	// Get a publisher
	pub, err := r.Publisher("test")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer pub.Close()

	// Get a consumer
	cons, err := r.Consumer("test")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer cons.Close()

	// Send a message
	for i := 0; i < 5; i++ {
		err = pub.Publish(string(i))
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
	}

	// Try to get the message
	var in string
	for i := 0; i < 5; i++ {
		err = cons.Consume(&in)
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
		if in != string(i) {
			t.Fatalf("unexpected msg! %v %v", in, i)
		}
	}

	// Nack all the messages
	err = cons.Nack()
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Should redeliver
	for i := 0; i < 5; i++ {
		err = cons.ConsumeAck(&in)
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
		if in != string(i) {
			t.Fatalf("unexpected msg! %v %v", in, i)
		}
	}
}

func TestCloseRelayInUse(t *testing.T) {
	CheckInteg(t)

	conf := Config{Addr: AMQPHost()}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	// Get a publisher
	pub, err := r.Publisher("close-in-use")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer pub.Close()

	// Get a consumer
	cons, err := r.Consumer("close-in-use")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer cons.Close()

	wg := &sync.WaitGroup{}
	wg.Add(3)

	// Send a message
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			err := pub.Publish(string(i))
			if err == ChannelClosed {
				break
			}
			if err != nil {
				t.Fatalf("unexpected err %s", err)
			}
		}
	}()

	// Should redeliver
	go func() {
		defer wg.Done()
		var in string
		for i := 0; i < 100; i++ {
			err := cons.ConsumeAck(&in)
			if err == ChannelClosed {
				break
			}
			if err != nil {
				t.Fatalf("unexpected err %s", err)
			}
			if in != string(i) {
				t.Fatalf("unexpected msg! %v %v", in, i)
			}
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond)
		err := r.Close()
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
	}()

	wg.Wait()
}

func TestClosePendingMsg(t *testing.T) {
	CheckInteg(t)

	conf := Config{Addr: AMQPHost(), PrefetchCount: 5, EnableMultiAck: true, DisablePublishConfirm: true}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	// Get a publisher
	pub, err := r.Publisher("pending-nack")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer pub.Close()

	// Get a consumer
	cons, err := r.Consumer("pending-nack")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Send a message
	for i := 0; i < 20; i++ {
		err = pub.Publish(string(i))
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
	}

	// Try to get the message
	var in string
	err = cons.Consume(&in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Close. Should nack.
	cons.Close()

	// Get a consumer
	cons, err = r.Consumer("pending-nack")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Should redeliver
	for i := 0; i < 20; i++ {
		err = cons.ConsumeAck(&in)
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
		if in != string(i) {
			t.Fatalf("unexpected msg! %v %v", in, i)
		}
	}
}

func TestDoubleConsume(t *testing.T) {
	CheckInteg(t)

	conf := Config{Addr: AMQPHost()}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	// Get a publisher
	pub, err := r.Publisher("double-cons")
	defer pub.Close()

	// Get a consumer
	cons, err := r.Consumer("double-cons")
	defer cons.Close()

	pub.Publish("test")
	var in string
	err = cons.Consume(&in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	err = cons.Consume(&in)
	if err.Error() != "Ack required before consume!" {
		t.Fatalf("unexpected err %s", err)
	}
}

func TestCloseConsume(t *testing.T) {
	CheckInteg(t)

	conf := Config{Addr: AMQPHost()}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	// Get a consumer
	cons, err := r.Consumer("double-cons")
	cons.Close()

	var in string
	err = cons.Consume(&in)
	if err != ChannelClosed {
		t.Fatalf("unexpected err %s", err)
	}

	err = cons.Ack()
	if err != ChannelClosed {
		t.Fatalf("unexpected err %s", err)
	}

	err = cons.Nack()
	if err != ChannelClosed {
		t.Fatalf("unexpected err %s", err)
	}
}

func TestClosePublish(t *testing.T) {
	CheckInteg(t)

	conf := Config{Addr: AMQPHost()}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	// Get a consumer
	pub, err := r.Publisher("double-cons")
	pub.Close()

	err = pub.Publish("test")
	if err != ChannelClosed {
		t.Fatalf("unexpected err %s", err)
	}
}

func TestNoHost(t *testing.T) {
	// Hopefully no rabbit there....
	conf := Config{Addr: "127.0.0.1", Port: 1}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	// Try to get a consumer
	_, err = r.Consumer("test")
	if err == nil {
		t.Fatalf("expected err!")
	}

	// Try to get a publisher
	_, err = r.Publisher("test")
	if err == nil {
		t.Fatalf("expected err!")
	}
}
