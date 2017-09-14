package bingodb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var (
	projectPath = filepath.Join(os.Getenv("GOPATH"), "/src/github.com/zoyi/bingodb/")
)

func TestErrorParseConfigByWrongPath(t *testing.T) {
	bingo := newBingo()
	absPath, _ := filepath.Abs(filepath.Join(projectPath, "/config", "werid_path_config.yml"))

	if err := ParseConfig(bingo, absPath); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestParseConfig(t *testing.T) {
	bingo := newBingo()
	absPath, _ := filepath.Abs(filepath.Join(projectPath, "/config", "test.yml"))

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
        hashKey: 'name'
        sortKey: 'email'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, configString); err != nil {
		t.Error(err)
	}
}

func TestErrorForEmptyConfig(t *testing.T) {
	weirdFieldConfig := `
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorForEmptyTable(t *testing.T) {
	weirdFieldConfig := `
tables:
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorForUnknownField(t *testing.T) {
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
    HashKey: 'name'
    sortKey: 'id'
    subIndices:
      friends:
        HashKey: 'email'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenFieldsIsEmpty(t *testing.T) {
	weirdFieldConfig := `
tables:
  weird:
    expireKey: 'expiresAt'
    HashKey: 'name'
    sortKey: 'id'
    subIndices:
      friends:
        HashKey: 'email'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorForUnknownFieldType(t *testing.T) {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'weird'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    HashKey: 'name'
    sortKey: 'id'
    subIndices:
      friends:
        HashKey: 'email'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenHashKeyIsEmpty(t *testing.T) {
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
        HashKey: 'email'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenHashKeyIsNotInField(t *testing.T) {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    HashKey: 'weird'
    sortKey: 'name'
    subIndices:
      friends:
        HashKey: 'email'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenSortKeyIsEmpty(t *testing.T) {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    HashKey: 'name'
    subIndices:
      friends:
        HashKey: 'email'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenSortKeyIsNotInField(t *testing.T) {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    HashKey: 'id'
    sortKey: 'weird'
    subIndices:
      friends:
        HashKey: 'email'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenHashKeySortKeyIsSame(t *testing.T) {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    HashKey: 'id'
    sortKey: 'id'
    subIndices:
      friends:
        HashKey: 'email'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenExpireKeyIsEmpty(t *testing.T) {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    HashKey: 'name'
    sortKey: 'id'
    subIndices:
      friends:
        HashKey: 'email'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenExpireKeyIsNotInField(t *testing.T) {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'weird'
    HashKey: 'id'
    sortKey: 'name'
    subIndices:
      friends:
        HashKey: 'email'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenExpireKeyTypeIsNotInteger(t *testing.T) {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'string'
    expireKey: 'expiresAt'
    HashKey: 'name'
    sortKey: 'id'
    subIndices:
      friends:
        HashKey: 'email'
        sortKey: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenSubIndicesHasWrongField(t *testing.T) {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    HashKey: 'name'
    sortKey: 'id'
    subIndices:
      friends:
        HashKey: 'email'
        weird: 'name'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenSubIndicesHashKeyEmpty(t *testing.T) {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    HashKey: 'name'
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

func TestErrorWhenSubIndicesSortKeyEmpty(t *testing.T) {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    HashKey: 'name'
    sortKey: 'id'
    subIndices:
      friends:
        HashKey: 'email'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenSubIndicesSameHashKeySortKey(t *testing.T) {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    HashKey: 'name'
    sortKey: 'id'
    subIndices:
      friends:
        HashKey: 'email'
        sortKey: 'email'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenSubIndicesHashKeyWrongValue(t *testing.T) {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    HashKey: 'name'
    sortKey: 'id'
    subIndices:
      friends:
        HashKey: 'weird'
        sortKey: 'email'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}

func TestErrorWhenSubIndicesSortKeyWrongValue(t *testing.T) {
	weirdFieldConfig := `
tables:
  weird:
    fields:
      id: 'string'
      name: 'string'
      email: 'string'
      expiresAt: 'integer'
    expireKey: 'expiresAt'
    HashKey: 'name'
    sortKey: 'id'
    subIndices:
      friends:
        HashKey: 'id'
        sortKey: 'weird'
`

	bingo := newBingo()

	if err := ParseConfigString(bingo, weirdFieldConfig); err != nil {
		fmt.Printf("Error occurred: [%v] - ok \n", err)
	} else {
		t.Fail()
	}
}
