package postgres

var _ SlotOffsetInfo = Slot("")

type Slot string

func (s Slot) Offset(lsn string) SlotOffset {
	return SlotOffset{
		SlotName: string(s),
		LSN:      lsn,
	}
}
func (s Slot) Zero() SlotOffset {
	return SlotOffset{
		SlotName: string(s),
		LSN:      StreamZeroOffset,
	}
}

func (s Slot) NeverDeliveredOffset() SlotOffset {
	return SlotOffset{
		SlotName: string(s),
		LSN:      StreamNeverDeliveredOffset,
	}
}

// getSlotOffset implements SlotOffsetInfo.
func (s Slot) getSlotOffset() SlotOffset {
	return SlotOffset{
		SlotName: string(s),
		LSN:      "",
	}
}
