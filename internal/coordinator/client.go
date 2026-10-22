// Derived from reqfleet/shibuya; modified for engine-sdk.
package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"

	"github.com/reqfleet/engine-sdk/protocol"
)

type Client struct {
	httpClient *http.Client
}

var ErrNon200Response = errors.New("received non-200 response from server")

type ReqOpts struct {
	Endpoint string
	APIKey   string
}

func NewClient(httpClient *http.Client) *Client {
	return &Client{httpClient: httpClient}
}

func (c *Client) FetchFile(ctx context.Context, ro ReqOpts, relativePath string) ([]byte, error) {
	resourceURL := fmt.Sprintf("https://%s%s", ro.Endpoint, path.Join(protocol.ArtifactPathPrefix, relativePath))
	req, err := http.NewRequestWithContext(ctx, "GET", resourceURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.sendRequest(req, ro)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (c *Client) sendRequest(req *http.Request, ro ReqOpts) (*http.Response, error) {
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", ro.APIKey))
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if err := handleResponse(resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func handleResponse(resp *http.Response) error {
	switch status := resp.StatusCode; {
	case status >= 400:
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		triggerErr := &protocol.TriggerError{}
		if json.Unmarshal(raw, triggerErr) == nil && triggerErr.Message != "" {
			return fmt.Errorf(
				"%w-resp: %w, status_code: %d",
				ErrNon200Response,
				triggerErr,
				status,
			)
		}
		return fmt.Errorf("%w-resp: %s, status_code: %d", ErrNon200Response, string(raw), status)
	case status == 200:
		return nil
	}
	return nil
}
