package query

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/skydb/sky/db"
)

// Ensure that we can encode queries.
func TestQueryEncodeDecode(t *testing.T) {
	table := createTempTable(t)
	table.Open()
	defer table.Close()

	table.Update(func(tx *db.Tx) error {
		json := `{"prefix":"","statements":[{"expression":"@baz == \"hello\"","statements":[{"dimensions":[],"fields":[{"aggregation":"sum","distinct":false,"expression":"@x","name":"myValue"}],"name":"xyz","type":"selection"}],"type":"condition","within":[0,2],"withinUnits":"steps"},{"dimensions":["@foo","@bar"],"fields":[{"aggregation":"count","distinct":false,"name":"count"}],"name":"","type":"selection"}]}` + "\n"

		// Decode
		q := NewQuery()
		q.Tx = tx
		buffer := bytes.NewBufferString(json)
		err := q.Decode(buffer)
		if err != nil {
			t.Fatalf("Query decoding error: %v", err)
		}

		// Encode
		buffer = new(bytes.Buffer)
		q.Encode(buffer)
		if buffer.String() != json {
			t.Fatalf("Query encoding error:\nexp: %s\ngot: %s", json, buffer.String())
		}

		return nil
	})
}

func createTempTable(t *testing.T) *db.Table {
	path, err := ioutil.TempDir("", "")
	os.RemoveAll(path)

	table := db.NewTable("test", path)
	err = table.Create()
	if err != nil {
		t.Fatalf("Unable to create table: %v", err)
	}

	return table
}
