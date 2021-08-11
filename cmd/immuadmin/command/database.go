/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package immuadmin

import (
	"fmt"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (cl *commandline) database(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "database",
		Short:   "Issue all database commands",
		Aliases: []string{"d"},
		//PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		ValidArgs:         []string{"list", "create", "update", "use", "clean"},
	}

	ccd := &cobra.Command{
		Use:               "list",
		Short:             "List all databases",
		Aliases:           []string{"l"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immuClient.DatabaseList(cl.context)
			if err != nil {
				return err
			}
			c.PrintTable(
				cmd.OutOrStdout(),
				[]string{"Database Name"},
				len(resp.Databases),
				func(i int) []string {
					row := make([]string, 1)
					if cl.options.CurrentDatabase == resp.Databases[i].DatabaseName {
						row[0] += fmt.Sprintf("*")
					}
					row[0] += fmt.Sprintf("%s", resp.Databases[i].DatabaseName)
					return row
				},
				fmt.Sprintf("%d database(s)", len(resp.Databases)),
			)
			return nil
		},
		Args: cobra.ExactArgs(0),
	}

	cc := &cobra.Command{
		Use:               "create",
		Short:             "Create a new database",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		Example:           "create {database_name}",
		RunE: func(cmd *cobra.Command, args []string) error {
			settings, err := prepareDatabaseSettings(args[0], cmd.Flags())
			if err != nil {
				return err
			}

			if settings.Replica {
				c.PrintfColorW(cmd.OutOrStdout(), c.Yellow, "Replication is a work-in-progress feature. Not ready for production use\n")
			}

			if err := cl.immuClient.CreateDatabase(cl.context, settings); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "database '%s' (replica = %v) successfully created\n", args[0], settings.Replica)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cc.Flags().BoolP("replica", "r", false, "set database as a replica")
	cc.Flags().String("master-database", "", "set master database to be replicated")
	cc.Flags().String("master-address", "127.0.0.1", "set master address")
	cc.Flags().Uint32("master-port", 3322, "set master port")
	cc.Flags().String("replica-username", "", "set username used for replication")
	cc.Flags().String("replica-password", "", "set password used for replication")

	cu := &cobra.Command{
		Use:               "update",
		Short:             "Update database",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		Example:           "update {database_name}",
		RunE: func(cmd *cobra.Command, args []string) error {
			settings, err := prepareDatabaseSettings(args[0], cmd.Flags())
			if err != nil {
				return err
			}

			if settings.Replica {
				c.PrintfColorW(cmd.OutOrStdout(), c.Yellow, "Replication is a work-in-progress feature. Not ready for production use\n")
			}

			if err := cl.immuClient.UpdateDatabase(cl.context, settings); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "database '%s' (replica = %v) successfully updated\n", args[0], settings.Replica)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cu.Flags().BoolP("replica", "r", false, "set database as a replica")
	cu.Flags().String("master-database", "", "set master database to be replicated")
	cu.Flags().String("master-address", "127.0.0.1", "set master address")
	cu.Flags().Uint32("master-port", 3322, "set master port")
	cu.Flags().String("replica-username", "", "set username used for replication")
	cu.Flags().String("replica-password", "", "set password used for replication")

	ccu := &cobra.Command{
		Use:               "use command",
		Short:             "Select database",
		Example:           "use {database_name}",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		ValidArgs:         []string{"databasename"},
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immuClient.UseDatabase(cl.context, &schema.Database{
				DatabaseName: args[0],
			})
			if err != nil {
				cl.quit(err)
			}
			if err != nil {
				return err
			}
			cl.immuClient.GetOptions().CurrentDatabase = args[0]
			if err = cl.ts.SetToken(args[0], resp.Token); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Now using %s\n", args[0])
			return nil
		},
		Args: cobra.MaximumNArgs(2),
	}

	ccc := &cobra.Command{
		Use:               "clean command",
		Short:             "Clean database index",
		Example:           "clean",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		ValidArgs:         []string{"databasename"},
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cl.immuClient.CleanIndex(cl.context, &emptypb.Empty{})
			if err != nil {
				cl.quit(err)
			}
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Database index successfully compacted\n")
			return nil
		},
		Args: cobra.ExactArgs(0),
	}

	ccmd.AddCommand(ccc)
	ccmd.AddCommand(ccu)
	ccmd.AddCommand(ccd)
	ccmd.AddCommand(cc)
	ccmd.AddCommand(cu)
	cmd.AddCommand(ccmd)
}

func prepareDatabaseSettings(db string, flags *pflag.FlagSet) (*schema.DatabaseSettings, error) {
	isReplica, err := flags.GetBool("replica")
	if err != nil {
		return nil, err
	}

	if !isReplica {
		return &schema.DatabaseSettings{DatabaseName: db}, nil
	}

	masterDatabase, err := flags.GetString("master-database")
	if err != nil {
		return nil, err
	}

	masterAddress, err := flags.GetString("master-address")
	if err != nil {
		return nil, err
	}

	masterPort, err := flags.GetUint32("master-port")
	if err != nil {
		return nil, err
	}

	replicaUsername, err := flags.GetString("replica-username")
	if err != nil {
		return nil, err
	}

	replicaPassword, err := flags.GetString("replica-username")
	if err != nil {
		return nil, err
	}

	return &schema.DatabaseSettings{
		DatabaseName:    db,
		Replica:         isReplica,
		MasterDatabase:  masterDatabase,
		MasterAddress:   masterAddress,
		MasterPort:      masterPort,
		ReplicaUsername: replicaUsername,
		ReplicaPassword: replicaPassword,
	}, nil
}
