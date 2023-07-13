// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datastore

import (
	"beam.apache.org/playground/backend/internal/constants"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/logger"
	"beam.apache.org/playground/backend/internal/utils"
	"cloud.google.com/go/datastore"
	"context"
	"errors"
)

// GetCurrentDbMigrationVersion returns the current version of the schema
func (d *Datastore) GetCurrentDbMigrationVersion(ctx context.Context) (int, error) {
	query := datastore.NewQuery(constants.SchemaKind).
		Namespace(utils.GetNamespace(ctx)).Order("-version").Limit(1)
	var schemas []*entity.SchemaEntity
	if _, err := d.Client.GetAll(ctx, query, &schemas); err != nil {
		logger.Errorf("Datastore: GetCurrentDbMigrationVersion(): error during getting current version, err: %s\n", err.Error())
		return -1, err
	}
	if len(schemas) == 0 {
		logger.Errorf("Datastore: GetCurrentDbMigrationVersion(): no schema versions found\n")
		return -1, errors.New("no schema versions found")
	}
	return schemas[0].Version, nil
}

// HasSchemaVersion returns true if the schema version is applied
func (d *Datastore) hasSchemaVersion(ctx context.Context, version int) (bool, error) {
	key := utils.GetSchemaVerKey(ctx, version)
	schemaEntity := new(entity.SchemaEntity)
	err := d.Client.Get(ctx, key, schemaEntity)
	if err != nil {
		if err == datastore.ErrNoSuchEntity {
			return false, nil
		}
		logger.Errorf("Datastore: hasSchemaVersion(): error during getting schema version, err: %s\n", err.Error())
		return false, err
	}
	logger.Infof("Datastore: hasSchemaVersion(): found SchemaEntity: %v\n", schemaEntity)
	return true, nil
}

// applyMigration applies the given migration to the database.
func (d *Datastore) applyMigration(ctx context.Context, migration Migration, sdkConfigPath string) error {
	logger.Infof("Datastore: applyMigration(): applying migration \"%d: %s\"\n", migration.GetVersion(), migration.GetDescription())

	_, err := d.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		if err := migration.Apply(ctx, tx, sdkConfigPath); err != nil {
			logger.Errorf("Datastore: applyMigration(): error during migration \"%d: %s\" applying, rolling back, err: %s\n",
				migration.GetVersion(),
				migration.GetDescription(),
				err.Error())
			return err
		}

		// Record the migration version
		if err := putSchemaVersion(ctx, tx, migration.GetVersion(), migration.GetDescription()); err != nil {
			logger.Errorf("Datastore: applyMigration(): error during migration \"%d: %s\" applying, rolling back, err: %s\n",
				migration.GetVersion(),
				migration.GetDescription(),
				err.Error())
			return err
		}

		return nil
	})

	if err != nil {
		logger.Errorf("Datastore: applyMigration(): error during migration \"%d: %s\" applying, err: %s\n",
			migration.GetVersion(),
			migration.GetDescription(),
			err.Error())
		return err
	}

	logger.Infof("Datastore: applyMigration(): migration \"%d: %s\" applied successfully\n", migration.GetVersion(), migration.GetDescription())
	return nil
}

// ApplyMigrations applies all migrations to the database.
func (d *Datastore) ApplyMigrations(ctx context.Context, migrations []Migration, sdkConfigPath string) error {
	for _, migration := range migrations {
		if applied, err := d.hasSchemaVersion(ctx, migration.GetVersion()); err != nil {
			logger.Errorf("Datastore: ApplyMigrations(): Error checking migration \"%d: %s\" : %s", migration.GetVersion(), migration.GetDescription(), err.Error())
			return err
		} else if applied {
			logger.Infof("Datastore: ApplyMigrations(): migration \"%d: %s\" already applied, skipping\n", migration.GetVersion(), migration.GetDescription())
			continue
		}

		if err := d.applyMigration(ctx, migration, sdkConfigPath); err != nil {
			logger.Errorf("Datastore: ApplyMigrations(): Error applying migration \"%d: %s\" : %s", migration.GetVersion(), migration.GetDescription(), err.Error())
			return err
		}
	}
	return nil
}

// putSchemaVersion puts the schema entity to datastore
func putSchemaVersion(ctx context.Context, tx *datastore.Transaction, version int, description string) error {
	key := utils.GetSchemaVerKey(ctx, version)
	if _, err := tx.Put(key, &entity.SchemaEntity{
		Version: version,
		Descr:   description,
	}); err != nil {
		logger.Errorf("Datastore: putSchemaVersion(): error during entity saving, err: %s\n", err.Error())
		return err
	}
	return nil
}

func (d *Datastore) PutSDKs(ctx context.Context, sdks []*entity.SDKEntity) error {
	_, err := d.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		return TxPutSDKs(ctx, tx, sdks)
	})

	if err != nil {
		logger.Errorf("Datastore: PutSDKs(): error during transaction, err: %s\n", err.Error())
		return err
	}

	return nil
}

// TxPutSDKs puts the SDK entity to datastore in a transaction
func TxPutSDKs(ctx context.Context, tx *datastore.Transaction, sdks []*entity.SDKEntity) error {
	if sdks == nil || len(sdks) == 0 {
		logger.Errorf("Datastore: TxPutSDKs(): sdks are empty")
		return nil
	}
	var keys []*datastore.Key
	for _, sdk := range sdks {
		keys = append(keys, utils.GetSdkKey(ctx, sdk.Name))
	}
	if _, err := tx.PutMulti(keys, sdks); err != nil {
		logger.Errorf("Datastore: TxPutSDKs(): error during entity saving, err: %s\n", err.Error())
		return err
	}
	return nil
}
