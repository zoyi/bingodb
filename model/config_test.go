package model

import (
	"testing"
	"path/filepath"
	"os"
	"fmt"
)

var (
	projectPath = filepath.Join(os.Getenv("GOPATH"), "/src/github.com/zoyi/bingodb/")
)

func TestParseConfig(t *testing.T) {
	bingo := newBingo()
	absPath, _ := filepath.Abs(filepath.Join(projectPath, "/config", "test_config.yml"))

	if err := ParseConfig(bingo, absPath); err != nil {
		t.Error(err)
	}
}

func TestParseConfigString(t *testing.T) {
	configString := `
tables:
  weired:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    hashKey: 'name'
    sortKey: 'id'
    subIndices:
      friends:
        hashKey: 'email'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, configString); err != nil {
		t.Error(err)
	}
}

func TestErrorWhenUnknownFieldType(t *testing.T)  {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string2'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    hashKey: 'name'
    sortKey: 'id'
    subIndices:
      friends:
        hashKey: 'email'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenUndefinedField(t *testing.T) {
	// tables > weired > expireKey is invalid
	weiredFieldConfig1 := `
tables:
  weired:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'weired'
    hashKey: 'name'
    sortKey: 'id'
    subIndices:
      friends:
        hashKey: 'email'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weiredFieldConfig1); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}

	// tables > weired > hashKey is invalid
	weiredFieldConfig2 := `
tables:
  weired:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    hashKey: 'weired'
    sortKey: 'id'
    subIndices:
      friends:
        hashKey: 'email'
        sortKey: 'name'
`

	if err := ParseConfigString(bingo, weiredFieldConfig2); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}

	// tables > weired > sortKey is invalid
	weiredFieldConfig3 := `
tables:
  weired:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    hashKey: 'name'
    sortKey: 'weired'
    subIndices:
      friends:
        hashKey: 'email'
        sortKey: 'name'
`

	if err := ParseConfigString(bingo, weiredFieldConfig3); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenUndefinedFieldOfSubIndices(t *testing.T) {
	// tables >> weired >> subIndices >> friends >> hashKey is invalid
	weiredSubIndicesConfig1 := `
tables:
  weired:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    hashKey: 'email'
    sortKey: 'id'
    subIndices:
      friends:
        hashKey: 'weired'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weiredSubIndicesConfig1); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}

	// tables >> weired >> subIndices >> friends >> sortKey is invalid
	weiredSubIndicesConfig2 := `
tables:
  weired:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    hashKey: 'email'
    sortKey: 'id'
    subIndices:
      friends:
        hashKey: 'email'
        sortKey: 'weired'
`

	if err := ParseConfigString(bingo, weiredSubIndicesConfig2); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}
