package model

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
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
  weird:
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

func TestErrorWhenUnknownField(t *testing.T)  {
	weirdFieldConfig := `
tables:
  weird:
    weird: 'weird'
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

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenUnknownFieldType(t *testing.T)  {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'weird'
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

func TestErrorWhenFieldsIsEmpty(t *testing.T)  {
	weirdFieldConfig := `
tables:
  weird:
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

func TestErrorWhenExpireKeyIsEmpty(t *testing.T)  {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
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

func TestErrorWhenHashKeyIsEmpty(t *testing.T)  {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
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

func TestErrorWhenSortKeyIsEmpty(t *testing.T)  {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    hashKey: 'name'
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

func TestErrorWhenSubIndicesHasWrongField(t *testing.T)  {
	weirdFieldConfig := `
tables:
  weird:
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
        weird: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenSubIndicesHashKeyEmpty(t *testing.T)  {
	weirdFieldConfig := `
tables:
  weird:
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
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenSubIndicesSortKeyEmpty(t *testing.T)  {
	weirdFieldConfig := `
tables:
  weird:
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
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenSubIndicesSameHashKeySortKey(t *testing.T)  {
	weirdFieldConfig := `
tables:
  weird:
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
        sortKey: 'email'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenSubIndicesWrongValue(t *testing.T)  {
	weirdFieldConfig := `
tables:
  weird:
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
        hashKey: 'weird'
        sortKey: 'email'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenExpireKeyTypeIsNotInteger(t *testing.T)  {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'string'
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
	// tables > weird > expireKey is invalid
	weirdFieldConfig1 := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'weird'
    hashKey: 'name'
    sortKey: 'id'
    subIndices:
      friends:
        hashKey: 'email'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig1); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}

	// tables > weird > hashKey is invalid
	weirdFieldConfig2 := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    hashKey: 'weird'
    sortKey: 'id'
    subIndices:
      friends:
        hashKey: 'email'
        sortKey: 'name'
`

	if err := ParseConfigString(bingo, weirdFieldConfig2); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}

	// tables > weird > sortKey is invalid
	weirdFieldConfig3 := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    hashKey: 'name'
    sortKey: 'weird'
    subIndices:
      friends:
        hashKey: 'email'
        sortKey: 'name'
`

	if err := ParseConfigString(bingo, weirdFieldConfig3); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenUndefinedFieldOfSubIndices(t *testing.T) {
	// tables >> weird >> subIndices >> friends >> hashKey is invalid
	weirdSubIndicesConfig1 := `
tables:
  weird:
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
        hashKey: 'weird'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdSubIndicesConfig1); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}

	// tables >> weird >> subIndices >> friends >> sortKey is invalid
	weirdSubIndicesConfig2 := `
tables:
  weird:
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
        sortKey: 'weird'
`

	if err := ParseConfigString(bingo, weirdSubIndicesConfig2); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}
