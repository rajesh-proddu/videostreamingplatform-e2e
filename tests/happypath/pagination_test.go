package happypath

import (
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestListVideos_Pagination(t *testing.T) {
	env := testutil.NewEnv(t)

	// Create 5 videos
	for i := 0; i < 5; i++ {
		env.CreateTestVideo(t, testutil.UniqueTitle("page"), 256)
	}

	t.Run("limit_2_returns_2", func(t *testing.T) {
		list, _, err := env.Metadata.ListVideos(2, 0)
		if err != nil {
			t.Fatalf("ListVideos failed: %v", err)
		}
		if len(list.Videos) != 2 {
			t.Errorf("got %d videos, want 2", len(list.Videos))
		}
	})

	t.Run("offset_skips_items", func(t *testing.T) {
		page1, _, _ := env.Metadata.ListVideos(3, 0)
		page2, _, _ := env.Metadata.ListVideos(3, 3)

		if len(page1.Videos) == 0 || len(page2.Videos) == 0 {
			t.Skip("not enough videos for offset test")
		}

		// Pages should have different videos
		for _, v1 := range page1.Videos {
			for _, v2 := range page2.Videos {
				if v1.ID == v2.ID {
					t.Errorf("page 1 and page 2 overlap on video %s", v1.ID)
				}
			}
		}
	})

	t.Run("zero_limit_returns_default", func(t *testing.T) {
		list, _, err := env.Metadata.ListVideos(0, 0)
		if err != nil {
			t.Fatalf("ListVideos failed: %v", err)
		}
		if len(list.Videos) == 0 {
			t.Error("zero limit should return default page, got 0 videos")
		}
	})
}
