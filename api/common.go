package api

import (
	"encoding/json"
	"net/http"
)

func getCMOTS(url string) (map[string]any, error) {
	bearerToken := "Bearer FkLpiVedKizrjkML771_wJ-vEKMPKVKrNzZHSAe2yipPt8jDyssu-l8GOVh1UrZs8dI05kNT_Jyjf7-Hi9Q7QDLaod844f_wb31hxDtBpWcf3DekV1AsIGifKUJJePgRw8BzC-xg-7Vb0ylK8YbgY72JYYPNFp-Vqs6xqA0W0wsGo9ouu2CXf5MPHW7qLrMdpQjLGp6EZJIKVGNloAvjfnhKoajHqVoUiAUbpZJfM-o6epe-edbRRN5WxN2FuIVPoEA9v-Uh_LIK5k5p9wm5xx5cww72r1uc3SD3TSo2nosdhreIFCcyMxLGNzG-In0f"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", bearerToken)

	client := http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var data map[string]any
	err = json.NewDecoder(res.Body).Decode(&data)
	if err != nil {
		return nil, err
	}

	return data, nil
}
