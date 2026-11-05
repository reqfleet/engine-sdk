package agent

import (
	"crypto/x509"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func unsetOptionEnv(t *testing.T, key string) {
	t.Helper()
	// Setenv registers restoration of the original value, including its absence.
	t.Setenv(key, "")
	require.NoError(t, os.Unsetenv(key))
}

func TestLoadAgentServerOptionsLabelLimit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   string
		unset   bool
		want    int
		invalid bool
	}{
		{name: "unset", unset: true, want: 10},
		{name: "empty", want: 10},
		{name: "zero", value: "0", want: 0},
		{name: "positive", value: "23", want: 23},
		{name: "positive sign", value: "+1", want: 1},
		{name: "positive zero", value: "+0", want: 0},
		{name: "negative zero", value: "-0", want: 0},
		{name: "leading zeros", value: "010", want: 10},
		{name: "maximum int", value: strconv.Itoa(math.MaxInt), want: math.MaxInt},
		{name: "negative", value: "-1", invalid: true},
		{name: "sign only", value: "+", invalid: true},
		{name: "whitespace", value: " ", invalid: true},
		{name: "leading whitespace", value: " 1", invalid: true},
		{name: "trailing whitespace", value: "1\n", invalid: true},
		{name: "malformed", value: "one", invalid: true},
		{name: "fraction", value: "1.5", invalid: true},
		{name: "hexadecimal", value: "0x10", invalid: true},
		{name: "overflow", value: strconv.FormatUint(uint64(math.MaxInt)+1, 10), invalid: true},
		{name: "negative overflow", value: "-999999999999999999999999999999", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.unset {
				unsetOptionEnv(t, "label_limit")
			} else {
				t.Setenv("label_limit", tc.value)
			}
			input := AgentServerOptions{
				EngineMeta:         EngineMeta{EngineID: "0"},
				CoordinatorRootCAs: x509.NewCertPool(),
				LabelsUsageLimit:   77,
			}
			options, err := LoadAgentServerOptions(input)
			if tc.invalid {
				require.ErrorContains(t, err, "label_limit")
				require.Equal(t, AgentServerOptions{}, options)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, options.LabelsUsageLimit)
		})
	}
}

func TestLoadAgentServerOptionsCoordinatorCA(t *testing.T) {
	ca, _, caPEM := newCoordinatorTestCA(t)
	for _, tc := range []struct {
		name  string
		value string
		unset bool
		valid bool
	}{
		{name: "unset", unset: true},
		{name: "empty"},
		{name: "whitespace", value: " \t\n\r "},
		{name: "malformed PEM", value: "-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----"},
		{name: "trusted CA", value: caPEM, valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("label_limit", "0")
			if tc.unset {
				unsetOptionEnv(t, "coordinator_ca_cert")
			} else {
				t.Setenv("coordinator_ca_cert", tc.value)
			}
			options, err := LoadAgentServerOptions(AgentServerOptions{EngineMeta: EngineMeta{EngineID: "0"}})
			if !tc.valid {
				require.ErrorContains(t, err, "coordinator_ca_cert")
				require.Equal(t, AgentServerOptions{}, options)
				return
			}
			require.NoError(t, err)
			_, err = ca.Verify(x509.VerifyOptions{Roots: options.CoordinatorRootCAs})
			require.NoError(t, err)
		})
	}
}

func TestLoadAgentServerOptionsEngineMeta(t *testing.T) {
	for _, tc := range []struct {
		name       string
		engineName string
		offset     string
		unset      string
		wantID     string
		errField   string
	}{
		{name: "identity and offset", engineName: "engine-test-zone-pool-2", offset: "3", wantID: "5"},
		{name: "zero identity", engineName: "engine-test-zone-pool-0", offset: "0", wantID: "0"},
		{name: "signed integers", engineName: "engine-test-zone-pool-+2", offset: "-2", wantID: "0"},
		{name: "unset name", unset: "engine_name", offset: "0", errField: "engine_name"},
		{name: "empty name", offset: "0", errField: "engine_name"},
		{name: "short name", engineName: "engine-test-2", offset: "0", errField: "engine_name"},
		{name: "long name", engineName: "engine-test-zone-extra-pool-2", offset: "0", errField: "engine_name"},
		{name: "empty suffix", engineName: "engine-test-zone-pool-", offset: "0", errField: "engine_name"},
		{name: "malformed suffix", engineName: "engine-test-zone-pool-a", offset: "0", errField: "engine_name"},
		{name: "overflowing suffix", engineName: "engine-test-zone-pool-999999999999999999999999999999", offset: "0", errField: "engine_name"},
		{name: "unset offset", engineName: "engine-test-zone-pool-2", unset: "offset", errField: "offset"},
		{name: "empty offset", engineName: "engine-test-zone-pool-2", errField: "offset"},
		{name: "malformed offset", engineName: "engine-test-zone-pool-2", offset: "a", errField: "offset"},
		{name: "overflowing offset", engineName: "engine-test-zone-pool-2", offset: "999999999999999999999999999999", errField: "offset"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("engine_name", tc.engineName)
			t.Setenv("offset", tc.offset)
			if tc.unset != "" {
				unsetOptionEnv(t, tc.unset)
			}
			t.Setenv("coordinator_ip", "coordinator.internal")
			t.Setenv("collection_id", "collection")
			t.Setenv("zone", "zone")
			t.Setenv("plan_id", "plan")
			t.Setenv("api_key", "secret")
			t.Setenv("label_limit", "0")
			options, err := LoadAgentServerOptions(AgentServerOptions{CoordinatorRootCAs: x509.NewCertPool()})
			if tc.errField != "" {
				require.ErrorContains(t, err, tc.errField)
				require.Equal(t, AgentServerOptions{}, options)
				return
			}
			require.NoError(t, err)
			require.Equal(t, EngineMeta{
				CoordinatorIP: "coordinator.internal", CollectionID: "collection",
				Zone: "zone", PlanID: "plan", EngineID: tc.wantID, APIKey: "secret",
			}, options.EngineMeta)
		})
	}
}

func TestLoadAgentServerOptionsEmptyMetadataFields(t *testing.T) {
	for _, unset := range []bool{false, true} {
		t.Run(strconv.FormatBool(unset), func(t *testing.T) {
			for _, key := range []string{"coordinator_ip", "collection_id", "zone", "plan_id", "api_key"} {
				if unset {
					unsetOptionEnv(t, key)
				} else {
					t.Setenv(key, "")
				}
			}
			t.Setenv("engine_name", "engine-test-zone-pool-0")
			t.Setenv("offset", "0")
			t.Setenv("label_limit", "0")
			options, err := LoadAgentServerOptions(AgentServerOptions{CoordinatorRootCAs: x509.NewCertPool()})
			require.NoError(t, err)
			require.Equal(t, EngineMeta{EngineID: "0"}, options.EngineMeta)
		})
	}
}

func TestLoadAgentServerOptionsPrecedence(t *testing.T) {
	t.Setenv("coordinator_ca_cert", "invalid")
	t.Setenv("engine_name", "invalid")
	t.Setenv("offset", "invalid")
	t.Setenv("coordinator_ip", "ignored.internal")
	t.Setenv("api_key", "ignored-secret")
	t.Setenv("label_limit", "0")
	input := AgentServerOptions{
		// Even a partial explicit metadata struct bypasses environment loading.
		EngineMeta:         EngineMeta{APIKey: "explicit-secret"},
		CoordinatorRootCAs: x509.NewCertPool(),
		LabelsUsageLimit:   77,
	}
	options, err := LoadAgentServerOptions(input)
	require.NoError(t, err)
	require.Equal(t, input.EngineMeta, options.EngineMeta)
	require.Same(t, input.CoordinatorRootCAs, options.CoordinatorRootCAs)
	require.Zero(t, options.LabelsUsageLimit)
}

func TestResolvedOptionsDoNotReloadEnvironment(t *testing.T) {
	ca, _, caPEM := newCoordinatorTestCA(t)
	t.Setenv("coordinator_ca_cert", caPEM)
	t.Setenv("engine_name", "engine-test-zone-pool-2")
	t.Setenv("offset", "3")
	t.Setenv("coordinator_ip", "coordinator.internal")
	t.Setenv("collection_id", "collection")
	t.Setenv("zone", "zone")
	t.Setenv("plan_id", "plan")
	t.Setenv("api_key", "resolved-secret")
	t.Setenv("label_limit", "4")
	options, err := LoadAgentServerOptions(AgentServerOptions{})
	require.NoError(t, err)

	// An adapter can make an explicit zero override after loading. Construction
	// must neither default that zero nor reread changed or invalid environment.
	options.LabelsUsageLimit = 0
	for _, key := range []string{"coordinator_ca_cert", "engine_name", "offset", "label_limit"} {
		t.Setenv(key, "invalid")
	}
	t.Setenv("api_key", "changed-secret")
	server := NewAgentServer(options)
	require.Zero(t, server.labelsUsageLimit)
	require.Equal(t, "5", server.options.EngineMeta.EngineID)
	_, err = ca.Verify(x509.VerifyOptions{Roots: server.options.CoordinatorRootCAs})
	require.NoError(t, err)
	for _, tc := range []struct {
		key  string
		code int
	}{
		{key: "resolved-secret", code: http.StatusOK},
		{key: "changed-secret", code: http.StatusForbidden},
	} {
		request := httptest.NewRequest(http.MethodGet, "/progress", nil)
		request.Header.Set("Authorization", "Bearer "+tc.key)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		require.Equal(t, tc.code, response.Code)
	}
}
