package main

import (
	"fmt"
	"regexp"
	"testing"
)

func TestMakeUuid(t *testing.T) {
	for i := 0; i < 16; i++ {
		t.Run("testMakeUuid", testMakeUUIDOnce)
	}
}

func testMakeUUIDOnce(t *testing.T) {
	uuid, err := makeUUID()
	if err != nil {
		if uuid != "" {
			t.FailNow()
		}
		return
	}
	match, err := regexp.MatchString("^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$", uuid)
	if !match {
		t.Fatal("UUID does not match expected format", uuid)
	}
}

func TestGetUniqueScriptName(t *testing.T) {
	script := "foo-bar.sh"
	scriptPath := "foo/" + script
	expectedFormat := fmt.Sprintf("^__sshtool_[^_]+_%s$", regexp.QuoteMeta(script))
	uniqueName := getUniqueScriptName(scriptPath)
	match, _ := regexp.MatchString(expectedFormat, uniqueName)
	if !match {
		t.Fatal("Uniquified script name does not match expected format", uniqueName)
	}
	// Fails with probability 2^-128 for random uuids
	uniqueName2 := getUniqueScriptName(scriptPath)
	if uniqueName == uniqueName2 {
		t.Fatal("Uniquified script name not unique", uniqueName, uniqueName2)
	}
	match, _ = regexp.MatchString(expectedFormat, uniqueName2)
	if !match {
		t.Fatal("Unique-ified script name does not match expected format", uniqueName2)
	}
}
