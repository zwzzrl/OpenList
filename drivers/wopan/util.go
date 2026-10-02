package template

import (
	"time"

	"github.com/OpenListTeam/wopan-sdk-go"
)

// do others that not defined in Driver interface

// getSortOrder maps the storage sort rule to the order fields used by
// model.SortFiles, so the rule is applied locally to the whole listing while
// the WoPan list API keeps its own default order.
func (d *Wopan) getSortOrder() (string, string) {
	switch d.SortRule {
	case "name_desc":
		return "name", "desc"
	case "time_asc":
		return "modified", "asc"
	case "time_desc":
		return "modified", "desc"
	case "size_asc":
		return "size", "asc"
	case "size_desc":
		return "size", "desc"
	default:
		return "name", "asc"
	}
}

func (d *Wopan) getSpaceType() string {
	if d.FamilyID == "" {
		return wopan.SpaceTypePersonal
	}
	return wopan.SpaceTypeFamily
}

// 20230607214351
func getTime(str string) (time.Time, error) {
	loc := time.FixedZone("UTC+8", 8*60*60)
	return time.ParseInLocation("20060102150405", str, loc)
}
