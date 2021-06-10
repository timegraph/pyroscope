package storage

import (
	"github.com/google/uuid"
)

const installID = "installID"

func (s *Storage) InstallID() string {
	id, err := s.service.Query(installID)
	if err != nil {
		return "id-read-error"
	}
	if id == nil {
		id = []byte(uuid.New().String())
		if err := s.service.Update(installID, id); err != nil {
			return "id-write-error"
		}
	}

	return string(id)
}
