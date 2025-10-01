package authzed

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"errors"
	"time"

	"crdb-authzed-load-test/internal/config"
)

type CheckRequest struct {
    Consistency Consistency `json:"consistency"`
    Resource    Resource    `json:"resource"`
    Permission  string      `json:"permission"`
    Subject     Subject     `json:"subject"`
}

type Consistency struct {
    MinimizeLatency bool `json:"minimizeLatency,omitempty"`
    FullyConsistent bool `json:"fullyConsistent,omitempty"`
}

type CheckResponse struct {
	CheckedAt struct {
	    Token string `json:"token"`
	}`json:"checkedAt"`
	Permissionship string `json:"permissionship"`
	PartialCaveatInfo string `json:"partialCaveatInfo"`
	DebugTrace string `json:"debugTrace"`
	OptionalExpiresAt string `json:"optionalExpiresAt"`
}

type WriteRequest struct {
	Updates []Update `json:"updates"`
}

type Update struct {
	Operation   string       `json:"operation"`
	Relationship Relationship `json:"relationship"`
}

type Relationship struct {
	Resource   Resource `json:"resource"`
	Relation   string   `json:"relation"`
	Subject    Subject  `json:"subject"`
}

type Resource struct {
	ObjectType string `json:"objectType"`
	ObjectId   string `json:"objectId"`
}

type Subject struct {
	Object Resource `json:"object"`
}


type WriteResponse struct {
	WrittenAt struct {
	    Token string `json:"token"`
	}`json:"writtenAt"`
}

func CheckPermission(objectType, objectID, permission, subjectType, subjectID string) (bool, error) {
	reqBody := CheckRequest{
        Consistency: Consistency{
            //MinimizeLatency: true,
            FullyConsistent: true,
        },
        Resource: Resource{
            ObjectType: objectType,
            ObjectId:   objectID,
        },
        Permission: permission,
        Subject: Subject{
            Object: Resource{
                ObjectType: subjectType,
                ObjectId:   subjectID,
            },
        },
    }
	var resp *http.Response
	var req *http.Request
	var err error

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		fmt.Printf("❌ Error marshaling check request: %v\n", err)
		return false, err
	}

	url := *config.AppConfig.AuthZed.API + "/v1/permissions/check"
	req, err = http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
    req.Header.Add("Authorization", "Bearer " + *config.AppConfig.AuthZed.Key)

	client := &http.Client{}
	for attempt := 1; attempt <= 3; attempt++ {
	resp, err = client.Do(req)
		if err == nil && resp != nil && resp.StatusCode == 200 {
			break
		}
		if attempt < 3 {
			fmt.Printf("🔁 Retry %d: AuthZed check failed (status=%v, error=%v)\n", attempt, getStatus(resp), err)
			time.Sleep(1000 * time.Millisecond)
		}
	}

	if err != nil || resp == nil {
		fmt.Printf("❌ Final failure: AuthZed check failed after 3 attempts. Error: %v\n", err)
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("⚠️  Unexpected status from AuthZed: %d\nResponse body: %s\n", resp.StatusCode, string(body))
		return false, errors.New("⚠️  Unexpected status from AuthZed")
	}

	var checkResp CheckResponse
	if err := json.NewDecoder(resp.Body).Decode(&checkResp); err != nil {
		fmt.Printf("❌ Error decoding AuthZed check response: %v\n", err)
		return false, err
	}

    return checkResp.Permissionship == "PERMISSIONSHIP_HAS_PERMISSION", nil
}

func WriteTuple(objectType, objectID, relation, subjectType, subjectID string) error {
	tuple := WriteRequest{
            Updates: []Update{
                {
                    Operation: "OPERATION_TOUCH",
                    Relationship: Relationship{
                        Resource: Resource{
                            ObjectType: objectType,
                            ObjectId:   objectID,
                        },
                        Relation: relation,
                        Subject: Subject{
                            Object: Resource{
                                ObjectType: subjectType,
                                ObjectId:   subjectID,
                            },
                        },
                    },
                },
            },
        }


	jsonData, err := json.Marshal(tuple)
	if err != nil {
		return fmt.Errorf("failed to marshal tuple: %w", err)
	}

	url := *config.AppConfig.AuthZed.API + "/v1/relationships/write"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
    req.Header.Add("Authorization", "Bearer " + *config.AppConfig.AuthZed.Key)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("POST failed: status=%v body=%s", resp.StatusCode, string(body))
	}

    fmt.Printf("🔑  Permission '%s' granted to %s: %s for %s: %s\n", relation, subjectType, subjectID, objectType, objectID)
	return nil
}

func getStatus(resp *http.Response) int {
	if resp != nil {
		return resp.StatusCode
	}
	return 0
}
