package com.activeandroid;

/*
 * Copyright (C) 2010 Michael Pardo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.text.TextUtils;

import com.activeandroid.util.IOUtils;
import com.activeandroid.util.Log;
import com.activeandroid.util.NaturalOrderComparator;
import com.activeandroid.util.SQLiteUtils;
import com.activeandroid.util.SqlParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class DatabaseHelper extends SQLiteOpenHelper {
	//////////////////////////////////////////////////////////////////////////////////////
	// PUBLIC CONSTANTS
	//////////////////////////////////////////////////////////////////////////////////////

	public final static String MIGRATION_PATH = "migrations";

	//////////////////////////////////////////////////////////////////////////////////////
	// PRIVATE FIELDS
	//////////////////////////////////////////////////////////////////////////////////////

	private Configuration configuration;

	//////////////////////////////////////////////////////////////////////////////////////
	// CONSTRUCTORS
	//////////////////////////////////////////////////////////////////////////////////////

	public DatabaseHelper(Configuration configuration) {
		super(configuration.getContext(), configuration.getDatabaseName(), null, configuration.getDatabaseVersion());
		copyAttachedDatabase(configuration.getContext(), configuration.getDatabaseName());
		this.configuration = configuration;
	}

	//////////////////////////////////////////////////////////////////////////////////////
	// OVERRIDEN METHODS
	//////////////////////////////////////////////////////////////////////////////////////

	@Override
	public void onOpen(SQLiteDatabase db) {
		executePragmas(db);
	}

	@Override
	public void onCreate(SQLiteDatabase db) {
		executePragmas(db);
		executeCreate(db);
		executeMigrations(db, -1, db.getVersion());
		executeCreateIndex(db);
	}

	@Override
	public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
		executePragmas(db);
		executeCreate(db);
		executeMigrations(db, oldVersion, newVersion);
		mergeAttachedDatabase(db, configuration.getContext(), configuration.getDatabaseName());
	}

	//////////////////////////////////////////////////////////////////////////////////////
	// PRIVATE METHODS
	//////////////////////////////////////////////////////////////////////////////////////

	private void mergeAttachedDatabase(SQLiteDatabase db, Context context, String databaseName) {
		// backup with existing tables
		final File dbPath = context.getDatabasePath(databaseName);
		String backupDbPath = IOUtils.exportDatabase(dbPath.getAbsolutePath());
		if (backupDbPath == null) {
			// db backup failed
			return;
		}
		// overwrite db file from assets
		writeAssetDbFile(context, databaseName, dbPath);
		try {
			// get table list from backup db
			List<String> tables = new ArrayList<String>();
			Cursor cursor = db.rawQuery("select name from sqlite_master WHERE type='table'", null);
			if (cursor.moveToFirst()) {
				while (!cursor.isAfterLast()) {
					String tableName = cursor.getString(cursor.getColumnIndex("name"));
					tables.add(tableName);
					cursor.moveToNext();
				}
			}
			cursor.close();
			if (db.inTransaction() ) {
				db.endTransaction();
			}
			// attach backup db
			db.execSQL("attach database '" + backupDbPath + "' as old");
			// run insert or replace with transaction
			db.beginTransaction();
			for (String table : tables) {
				if ("sqlite_sequence".equals(table)) {
					continue;
				}
				String sql = "create table if not exists " + table + " as select * from old." + table;
				db.execSQL(sql);
			}
			db.setTransactionSuccessful();
		} catch (Exception e) {
			Log.e("Failed to attach db", e);
		} finally {
			db.endTransaction();
		}
	}

	private void copyAttachedDatabase(Context context, String databaseName) {
		final File dbPath = context.getDatabasePath(databaseName);

		// If the database already exists, return
		if (dbPath.exists()) {
			return;
		}

		// Make sure we have a path to the file
		dbPath.getParentFile().mkdirs();

		writeAssetDbFile(context, databaseName, dbPath);
	}

	private void writeAssetDbFile(Context context, String databaseName, File dbPath) {
		// Try to copy database file
		try {
			final InputStream inputStream = context.getAssets().open(databaseName);
			final OutputStream output = new FileOutputStream(dbPath);
			byte[] buffer = new byte[8192];
			int length;
			while ((length = inputStream.read(buffer, 0, 8192)) > 0) {
				output.write(buffer, 0, length);
			}
			output.flush();
			output.close();
			inputStream.close();
		}
		catch (IOException e) {
			Log.e("Failed to open file", e);
		}
	}

	private void executePragmas(SQLiteDatabase db) {
		if (SQLiteUtils.FOREIGN_KEYS_SUPPORTED) {
			db.execSQL("PRAGMA foreign_keys=ON;");
			Log.i("Foreign Keys supported. Enabling foreign key features.");
		}
	}

	private void executeCreateIndex(SQLiteDatabase db) {
		db.beginTransaction();
		try {
			for (TableInfo tableInfo : Cache.getTableInfos()) {
				String[] definitions = SQLiteUtils.createIndexDefinition(tableInfo);
				for (String definition : definitions) {
					db.execSQL(definition);
				}
			}
			db.setTransactionSuccessful();
		}
		finally {
			db.endTransaction();
		}
	}

	private void executeCreate(SQLiteDatabase db) {
		db.beginTransaction();
		try {
			for (TableInfo tableInfo : Cache.getTableInfos()) {
				db.execSQL(SQLiteUtils.createTableDefinition(tableInfo));
			}
			db.setTransactionSuccessful();
		}
		finally {
			db.endTransaction();
		}
	}

	private boolean executeMigrations(SQLiteDatabase db, int oldVersion, int newVersion) {
		boolean migrationExecuted = false;
		try {
			final List<String> files = Arrays.asList(Cache.getContext().getAssets().list(MIGRATION_PATH));
			Collections.sort(files, new NaturalOrderComparator());

			db.beginTransaction();
			try {
				for (String file : files) {
					try {
						final int version = Integer.valueOf(file.replace(".sql", ""));

						if (version > oldVersion && version <= newVersion) {
							executeSqlScript(db, file);
							migrationExecuted = true;

							Log.i(file + " executed succesfully.");
						}
					}
					catch (NumberFormatException e) {
						Log.w("Skipping invalidly named file: " + file, e);
					}
				}
				db.setTransactionSuccessful();
			}
			finally {
				db.endTransaction();
			}
		}
		catch (IOException e) {
			Log.e("Failed to execute migrations.", e);
		}

		return migrationExecuted;
	}

	private void executeSqlScript(SQLiteDatabase db, String file) {

		InputStream stream = null;

		try {
			stream = Cache.getContext().getAssets().open(MIGRATION_PATH + "/" + file);

			if (Configuration.SQL_PARSER_DELIMITED.equalsIgnoreCase(configuration.getSqlParser())) {
				executeDelimitedSqlScript(db, stream);

			} else {
				executeLegacySqlScript(db, stream);

			}

		} catch (IOException e) {
			Log.e("Failed to execute " + file, e);

		} finally {
			IOUtils.closeQuietly(stream);

		}
	}

	private void executeDelimitedSqlScript(SQLiteDatabase db, InputStream stream) throws IOException {

		List<String> commands = SqlParser.parse(stream);

		for(String command : commands) {
			db.execSQL(command);
		}
	}

	private void executeLegacySqlScript(SQLiteDatabase db, InputStream stream) throws IOException {

		InputStreamReader reader = null;
		BufferedReader buffer = null;

		try {
			reader = new InputStreamReader(stream);
			buffer = new BufferedReader(reader);
			String line = null;

			while ((line = buffer.readLine()) != null) {
				line = line.replace(";", "").trim();
				if (!TextUtils.isEmpty(line)) {
					db.execSQL(line);
				}
			}

		} finally {
			IOUtils.closeQuietly(buffer);
			IOUtils.closeQuietly(reader);

		}
	}
}
