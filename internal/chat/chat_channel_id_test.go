package chat

import (
	"testing"

	"github.com/google/uuid"
)

func TestChannelIdRoundTrip_Room(t *testing.T) {
	id, stream, err := BuildChannelId(nil, nil, "", "general", ChannelJoinRoom)
	if err != nil {
		t.Fatal(err)
	}
	if id != "2...general" {
		t.Fatalf("id=%q", id)
	}
	if stream.Mode != StreamModeChannel || stream.Label != "general" {
		t.Fatalf("stream=%+v", stream)
	}
	parsed, err := ChannelIdToStream(id)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Mode != StreamModeChannel || parsed.Label != "general" {
		t.Fatalf("parsed=%+v", parsed)
	}
}

func TestChannelIdRoundTrip_DM(t *testing.T) {
	a := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	b := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	id, stream, err := BuildChannelId(nil, nil, a, b, ChannelJoinDM)
	if err != nil {
		t.Fatal(err)
	}
	if stream.Mode != StreamModeDM {
		t.Fatalf("mode=%d", stream.Mode)
	}
	if stream.Subject != a || stream.Subcontext != b {
		t.Fatalf("expected sorted a,b got %s %s", stream.Subject, stream.Subcontext)
	}
	want := "4." + a + "." + b + "."
	if id != want {
		t.Fatalf("id=%q want=%q", id, want)
	}
	parsed, err := ChannelIdToStream(id)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Subject != a || parsed.Subcontext != b {
		t.Fatalf("parsed=%+v", parsed)
	}
}

func TestChannelIdRoundTrip_Group(t *testing.T) {
	gid := uuid.New().String()
	id, stream, err := BuildChannelId(nil, nil, "", gid, ChannelJoinGroup)
	if err != nil {
		t.Fatal(err)
	}
	if stream.Mode != StreamModeGroup || stream.Subject != gid {
		t.Fatalf("stream=%+v", stream)
	}
	want := "3." + gid + ".."
	if id != want {
		t.Fatalf("id=%q want=%q", id, want)
	}
}

func TestChannelIdInvalid(t *testing.T) {
	if _, err := ChannelIdToStream("1...room"); err == nil {
		t.Fatal("expected error for legacy join-type prefix")
	}
	if _, err := ChannelIdToStream("2.a.b.label"); err == nil {
		t.Fatal("expected error for room with subject")
	}
	if _, _, err := BuildChannelId(nil, nil, "", "", ChannelJoinRoom); err == nil {
		t.Fatal("expected empty target error")
	}
}

func TestStreamToChannelId_NilUUIDOmit(t *testing.T) {
	id, err := StreamToChannelId(Stream{Mode: StreamModeChannel, Label: "lobby"})
	if err != nil {
		t.Fatal(err)
	}
	if id != "2...lobby" {
		t.Fatalf("id=%q", id)
	}
}
