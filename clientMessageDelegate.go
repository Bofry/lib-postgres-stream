package postgres

var _ MessageDelegate = new(clientMessageDelegate)

type clientMessageDelegate struct {
	client *Consumer
}

// OnAck implements MessageDelegate.
func (d *clientMessageDelegate) OnAck(msg *Message) {
	err := d.client.doAck(msg.consumedXLogPos)
	if err != nil {
		d.client.Logger.Printf("SendStandbyStatusUpdate failed on (%s#%s): %+v", msg.SlotName, msg.consumedXLogPos, err)
	}
}
