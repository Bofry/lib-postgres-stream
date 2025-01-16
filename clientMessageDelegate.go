package postgres

var _ MessageDelegate = new(clientMessageDelegate)

type clientMessageDelegate struct {
	client *Consumer
}

// OnAck implements MessageDelegate.
func (d *clientMessageDelegate) OnAck(msg *Message) {
	if !msg.canAck() {
		return
	}

	err := d.client.doAck(msg.consumedXLogPos)
	if err != nil {
		d.client.Logger.Printf("SendStandbyStatusUpdate failed on (%s#%s): %+v", msg.Slot, msg.consumedXLogPos, err)
	}
}
