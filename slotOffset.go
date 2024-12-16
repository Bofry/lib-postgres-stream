package postgres

var _ SlotOffsetInfo = SlotOffset{}

type SlotOffset struct {
	SlotName string
	LSN      string
}

// getSlotOffset implements SlotOffsetInfo.
func (s SlotOffset) getSlotOffset() SlotOffset {
	return s
}
