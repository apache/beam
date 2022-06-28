package mapper

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	datastoreDb "beam.apache.org/playground/backend/internal/db/datastore"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/utils"
	"time"
)

type EntityMapper struct {
	appEnv *environment.ApplicationEnvs
}

func New(appEnv *environment.ApplicationEnvs) *EntityMapper {
	return &EntityMapper{appEnv: appEnv}
}

func (m *EntityMapper) ToSnippet(info *pb.SaveSnippetRequest) *entity.Snippet {
	nowDate := time.Now()
	snippet := entity.Snippet{
		IDMeta: &entity.IDMeta{
			Salt:     m.appEnv.PlaygroundSalt(),
			IdLength: m.appEnv.IdLength(),
		},
		//OwnerId property will be used in Tour of Beam project
		Snippet: &entity.SnippetEntity{
			SchVer:        utils.GetNameKey(datastoreDb.SchemaKind, m.appEnv.SchemaVersion(), datastoreDb.Namespace, nil),
			Sdk:           utils.GetNameKey(datastoreDb.SdkKind, info.Sdk.String(), datastoreDb.Namespace, nil),
			PipeOpts:      info.PipelineOptions,
			Created:       nowDate,
			LVisited:      nowDate,
			Origin:        entity.Origin(entity.OriginValue[m.appEnv.Origin()]),
			NumberOfFiles: len(info.Files),
		},
	}
	return &snippet
}

func (m *EntityMapper) ToFileEntity(info *pb.SaveSnippetRequest, file *pb.SnippetFile) *entity.FileEntity {
	var isMain bool
	if len(info.Files) == 1 {
		isMain = true
	} else {
		isMain = utils.IsFileMain(file.Content, info.Sdk)
	}
	return &entity.FileEntity{
		Name:     utils.GetFileName(file.Name, info.Sdk),
		Content:  file.Content,
		CntxLine: 1,
		IsMain:   isMain,
	}
}
