package model

import "testing"

func TestLoad(t *testing.T) {
	bingo, err := Load("test_config.yml")
	if err != nil {
		t.Error("error should be nil")
	}

	if bingo == nil {
		t.Error("bingo instance should not be nil")
	}

	bingo, err = Load("no_exist.yml")
	if err == nil {
		t.Error("error should not be nil")
	}

	if bingo != nil {
		t.Error("bingo instance should be nil")
	}
}
