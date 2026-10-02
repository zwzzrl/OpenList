package template

import (
	"slices"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/wopan-sdk-go"
)

// woPanTestObj builds a driver object the same way the listing does.
func woPanTestObj(t *testing.T, name string, size int64, createTime string, isDir bool) model.Obj {
	t.Helper()
	fileType := 1
	if isDir {
		fileType = 0
	}
	obj, err := fileToObj(wopan.File{
		Id:         "id-" + name,
		Fid:        "fid-" + name,
		Name:       name,
		Size:       size,
		CreateTime: createTime,
		Type:       fileType,
	})
	if err != nil {
		t.Fatalf("fileToObj(%q) failed: %v", name, err)
	}
	return obj
}

func names(objs []model.Obj) []string {
	res := make([]string, len(objs))
	for i, obj := range objs {
		res[i] = obj.GetName()
	}
	return res
}

func TestGetSortOrder(t *testing.T) {
	tests := []struct {
		rule          string
		wantBy        string
		wantDirection string
	}{
		{"name_asc", "name", "asc"},
		{"name_desc", "name", "desc"},
		{"time_asc", "modified", "asc"},
		{"time_desc", "modified", "desc"},
		{"size_asc", "size", "asc"},
		{"size_desc", "size", "desc"},
		{"", "name", "asc"},
		{"unexpected", "name", "asc"},
	}
	for _, tt := range tests {
		t.Run(tt.rule, func(t *testing.T) {
			d := &Wopan{Addition: Addition{SortRule: tt.rule}}
			by, direction := d.getSortOrder()
			if by != tt.wantBy || direction != tt.wantDirection {
				t.Fatalf("getSortOrder() = (%q, %q), want (%q, %q)",
					by, direction, tt.wantBy, tt.wantDirection)
			}
		})
	}
}

func TestSortFilesByRule(t *testing.T) {
	// Deliberately listed as b, a, c so an unsorted result is visible.
	newObjs := func(t *testing.T) []model.Obj {
		t.Helper()
		return []model.Obj{
			woPanTestObj(t, "b", 200, "20240103000000", false),
			woPanTestObj(t, "a", 300, "20240102000000", false),
			woPanTestObj(t, "c", 100, "20240101000000", false),
		}
	}
	tests := []struct {
		rule string
		want []string
	}{
		{"name_asc", []string{"a", "b", "c"}},
		{"name_desc", []string{"c", "b", "a"}},
		{"size_asc", []string{"c", "b", "a"}},
		{"size_desc", []string{"a", "b", "c"}},
		{"time_asc", []string{"c", "a", "b"}},
		{"time_desc", []string{"b", "a", "c"}},
		{"", []string{"a", "b", "c"}},
	}
	for _, tt := range tests {
		t.Run(tt.rule, func(t *testing.T) {
			objs := newObjs(t)
			d := &Wopan{Addition: Addition{SortRule: tt.rule}}
			by, direction := d.getSortOrder()
			model.SortFiles(objs, by, direction)
			if got := names(objs); !slices.Equal(got, tt.want) {
				t.Fatalf("sorted %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSortFilesByRuleEdgeCases(t *testing.T) {
	d := &Wopan{Addition: Addition{SortRule: "name_asc"}}
	by, direction := d.getSortOrder()

	t.Run("empty", func(t *testing.T) {
		var objs []model.Obj
		model.SortFiles(objs, by, direction)
		if len(objs) != 0 {
			t.Fatalf("empty listing became %v", names(objs))
		}
	})

	t.Run("single", func(t *testing.T) {
		objs := []model.Obj{woPanTestObj(t, "only", 1, "20240101000000", true)}
		model.SortFiles(objs, by, direction)
		if got := names(objs); !slices.Equal(got, []string{"only"}) {
			t.Fatalf("single entry listing became %v", got)
		}
	})

	t.Run("equal keys", func(t *testing.T) {
		objs := []model.Obj{
			woPanTestObj(t, "a", 0, "20240101000000", true),
			woPanTestObj(t, "b", 0, "20240101000000", true),
			woPanTestObj(t, "c", 0, "20240101000000", true),
		}
		model.SortFiles(objs, by, direction)
		got := names(objs)
		slices.Sort(got)
		if !slices.Equal(got, []string{"a", "b", "c"}) {
			t.Fatalf("sorting dropped or duplicated entries: %v", got)
		}
	})
}

func TestFileToObj(t *testing.T) {
	obj, err := fileToObj(wopan.File{
		Id:         "id-1",
		Fid:        "fid-1",
		Name:       "movie.mkv",
		Size:       12345,
		CreateTime: "20230607214351",
		Type:       1,
		ThumbUrl:   "https://example.com/thumb.jpg",
	})
	if err != nil {
		t.Fatalf("fileToObj failed: %v", err)
	}
	file, ok := obj.(*Object)
	if !ok {
		t.Fatalf("fileToObj returned %T, want *Object", obj)
	}
	if file.GetID() != "id-1" || file.GetName() != "movie.mkv" || file.GetSize() != 12345 {
		t.Fatalf("unexpected object: id=%q name=%q size=%d", file.GetID(), file.GetName(), file.GetSize())
	}
	if file.FID != "fid-1" {
		t.Fatalf("FID = %q, want %q", file.FID, "fid-1")
	}
	if file.IsDir() {
		t.Fatal("a file was reported as a folder")
	}
	want := time.Date(2023, 6, 7, 21, 43, 51, 0, time.FixedZone("UTC+8", 8*60*60))
	if !file.ModTime().Equal(want) {
		t.Fatalf("ModTime = %v, want %v", file.ModTime(), want)
	}

	dir, err := fileToObj(wopan.File{Id: "id-2", Name: "folder", CreateTime: "20230607214351", Type: 0})
	if err != nil {
		t.Fatalf("fileToObj failed for a folder: %v", err)
	}
	if !dir.IsDir() {
		t.Fatal("a folder was reported as a file")
	}
}

func TestFileToObjRejectsInvalidTime(t *testing.T) {
	if _, err := fileToObj(wopan.File{Id: "id-1", Name: "movie.mkv", CreateTime: "not-a-time"}); err == nil {
		t.Fatal("fileToObj accepted an invalid create time")
	}
}

func TestGetTime(t *testing.T) {
	got, err := getTime("20230607214351")
	if err != nil {
		t.Fatalf("getTime failed: %v", err)
	}
	if _, offset := got.Zone(); offset != 8*60*60 {
		t.Fatalf("getTime offset = %d, want %d", offset, 8*60*60)
	}
	want := time.Date(2023, 6, 7, 21, 43, 51, 0, time.FixedZone("UTC+8", 8*60*60))
	if !got.Equal(want) {
		t.Fatalf("getTime = %v, want %v", got, want)
	}
}

func TestGetTimeRejectsInvalid(t *testing.T) {
	for _, input := range []string{"", "2023-06-07", "2023060721435", "abcdefghijklmn"} {
		t.Run(input, func(t *testing.T) {
			if _, err := getTime(input); err == nil {
				t.Fatalf("getTime(%q) accepted an invalid time", input)
			}
		})
	}
}

func TestGetSpaceType(t *testing.T) {
	if got := (&Wopan{}).getSpaceType(); got != wopan.SpaceTypePersonal {
		t.Fatalf("getSpaceType() without family id = %q, want %q", got, wopan.SpaceTypePersonal)
	}
	if got := (&Wopan{Addition: Addition{FamilyID: "123"}}).getSpaceType(); got != wopan.SpaceTypeFamily {
		t.Fatalf("getSpaceType() with family id = %q, want %q", got, wopan.SpaceTypeFamily)
	}
}
