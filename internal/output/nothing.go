package output


type Nothing struct {

}

func NewNothing() (*Nothing) {
	return &Nothing{}
}

func (f *Nothing) Write(bs []byte) (int, error) {
	return 0, nil
}

func (f *Nothing) Close() error {
	return nil
}
