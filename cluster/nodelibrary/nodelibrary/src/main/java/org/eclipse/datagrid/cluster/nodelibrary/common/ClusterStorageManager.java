package org.eclipse.datagrid.cluster.nodelibrary.common;

/*-
 * #%L
 * Eclipse DataGrid Cluster Nodelibrary
 * %%
 * Copyright (C) 2025 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 * 
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

import org.eclipse.serializer.afs.types.AFile;
import org.eclipse.serializer.collections.types.XGettingEnum;
import org.eclipse.serializer.meta.NotImplementedYetError;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceCommitListener;
import org.eclipse.serializer.persistence.types.PersistenceManager;
import org.eclipse.serializer.persistence.types.PersistenceRootsView;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryExporter;
import org.eclipse.serializer.persistence.types.Storer;
import org.eclipse.serializer.reference.Lazy;
import org.eclipse.store.storage.types.Database;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.eclipse.store.storage.types.StorageConnection;
import org.eclipse.store.storage.types.StorageEntityCacheEvaluator;
import org.eclipse.store.storage.types.StorageEntityTypeExportFileProvider;
import org.eclipse.store.storage.types.StorageEntityTypeExportStatistics;
import org.eclipse.store.storage.types.StorageEntityTypeHandler;
import org.eclipse.store.storage.types.StorageLiveFileProvider;
import org.eclipse.store.storage.types.StorageManager;
import org.eclipse.store.storage.types.StorageRawFileStatistics;
import org.eclipse.store.storage.types.StorageTypeDictionary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.datagrid.cluster.nodelibrary.common.exception.StorageLimitReachedException;

public interface ClusterStorageManager<T> extends StorageManager
{
	void activateDistribution();

	boolean isReady();

	boolean isDistributor();

	long getCurrentOffset();

	@Override
	Lazy<T> root();

	@Override
	ClusterStorageManager<T> start();

	public static abstract class Abstract<T> implements ClusterStorageManager<T>
	{
		private static final Logger LOG = LoggerFactory.getLogger(ClusterStorageManager.class);

		protected Abstract()
		{
		}

		private <R> R exitOnThrow(final Callable<R> callable)
		{
			try
			{
				return callable.call();
			}
			catch (final Throwable t)
			{
				// TODO (MS 29.11.2024): How many non-broken-storage exceptions are there? Like the PersistenceExceptionTypeNotPersistable exception? No need to exit on those, right?
				LOG.error("store() call failed. Exiting system.", t);
				System.exit(1);
				return null;
			}
		}

		private void exitOnThrow(final Runnable runnable)
		{
			exitOnThrow(() ->
			{
				runnable.run();
				return null;
			});
		}

		protected abstract StorageManager delegate();

		private void ensureStorageCapacity()
		{
			if (StorageLimitChecker.get().limitReached())
			{
				throw new StorageLimitReachedException(
					"Can not store more objects in storage as the storage limit has been reached"
				);
			}

		}

		@Override
		public void checkAcceptingTasks()
		{
			this.delegate().checkAcceptingTasks();
		}

		@Override
		public StorageConfiguration configuration()
		{
			return this.delegate().configuration();
		}

		@Override
		public StorageConnection createConnection()
		{
			// TODO: Create own StorageConnection type that also has the ensureStorageCapacity() and ensureDistribution() checks
			throw new NotImplementedYetError();
		}

		@Override
		public StorageRawFileStatistics createStorageStatistics()
		{
			return this.delegate().createStorageStatistics();
		}

		@Override
		public Database database()
		{
			return this.delegate().database();
		}

		@Override
		public void exportChannels(final StorageLiveFileProvider fileProvider, final boolean performGarbageCollection)
		{
			this.delegate().exportChannels(fileProvider, performGarbageCollection);
		}

		@Override
		public StorageEntityTypeExportStatistics exportTypes(
			final StorageEntityTypeExportFileProvider exportFileProvider,
			final Predicate<? super StorageEntityTypeHandler> isExportType
		)
		{
			return this.delegate().exportTypes(exportFileProvider, isExportType);
		}

		@Override
		public void importData(final XGettingEnum<ByteBuffer> importData)
		{
			this.delegate().importData(importData);
		}

		@Override
		public void importFiles(final XGettingEnum<AFile> importFiles)
		{
			this.delegate().importFiles(importFiles);
		}

		@Override
		public long initializationTime()
		{
			return this.delegate().initializationTime();
		}

		@Override
		public boolean isAcceptingTasks()
		{
			return this.delegate().isAcceptingTasks();
		}

		@Override
		public boolean isActive()
		{
			return this.delegate().isActive();
		}

		@Override
		public boolean isRunning()
		{
			return this.delegate().isRunning();
		}

		@Override
		public boolean isShuttingDown()
		{
			return this.delegate().isShuttingDown();
		}

		@Override
		public boolean isStartingUp()
		{
			return this.delegate().isStartingUp();
		}

		@Override
		public boolean issueCacheCheck(final long nanoTimeBudget, final StorageEntityCacheEvaluator entityEvaluator)
		{
			return this.delegate().issueCacheCheck(nanoTimeBudget, entityEvaluator);
		}

		@Override
		public boolean issueFileCheck(final long nanoTimeBudget)
		{
			return this.delegate().issueFileCheck(nanoTimeBudget);
		}

		@Override
		public void issueFullBackup(
			final StorageLiveFileProvider targetFileProvider,
			final PersistenceTypeDictionaryExporter typeDictionaryExporter
		)
		{
			this.delegate().issueFullBackup(targetFileProvider, typeDictionaryExporter);
		}

		@Override
		public boolean issueGarbageCollection(final long nanoTimeBudget)
		{
			return this.delegate().issueGarbageCollection(nanoTimeBudget);
		}

		@Override
		public void issueTransactionsLogCleanup()
		{
			this.delegate().issueTransactionsLogCleanup();
		}

		@Override
		public long operationModeTime()
		{
			return this.delegate().operationModeTime();
		}

		@Override
		public PersistenceManager<Binary> persistenceManager()
		{
			return this.delegate().persistenceManager();
		}

		@Override
		public Object setRoot(final Object newRoot)
		{
			return this.delegate().setRoot(newRoot);
		}

		@Override
		public boolean shutdown()
		{
			LOG.info("Shutting down.");
			return this.delegate().shutdown();
		}

		@Override
		public ClusterStorageManager<T> start()
		{
			this.delegate().start();
			return this;
		}

		@Override
		public long store(final Object instance)
		{
			this.ensureStorageCapacity();
			return this.exitOnThrow(() -> this.delegate().store(instance));
		}

		@Override
		public long[] storeAll(final Object... instances)
		{
			this.ensureStorageCapacity();
			return this.exitOnThrow(() -> this.delegate().storeAll(instances));
		}

		@Override
		public void storeAll(final Iterable<?> instances)
		{
			this.ensureStorageCapacity();
			this.exitOnThrow(() -> this.delegate().storeAll(instances));
		}

		@Override
		public long storeRoot()
		{
			this.ensureStorageCapacity();
			return this.exitOnThrow(() -> this.delegate().storeRoot());
		}

		@Override
		public StorageTypeDictionary typeDictionary()
		{
			return this.delegate().typeDictionary();
		}

		@Override
		public PersistenceRootsView viewRoots()
		{
			return this.delegate().viewRoots();
		}

		@Override
		public Storer createEagerStorer()
		{
			return new ClusterStorerAdapter(delegate().createEagerStorer());
		}

		@Override
		public Storer createLazyStorer()
		{
			return new ClusterStorerAdapter(delegate().createLazyStorer());
		}

		@Override
		public Storer createStorer()
		{
			return new ClusterStorerAdapter(delegate().createStorer());
		}

		private class ClusterStorerAdapter implements Storer
		{
			private final Storer storer;

			private ClusterStorerAdapter(final Storer storer)
			{
				this.storer = storer;
			}

			@Override
			public long store(Object instance)
			{
				return storer.store(instance);
			}

			@Override
			public long[] storeAll(Object... instances)
			{
				return storer.storeAll(instances);
			}

			@Override
			public void storeAll(Iterable<?> instances)
			{
				storer.storeAll(instances);
			}

			@Override
			public Object commit()
			{
				ensureStorageCapacity();
				return exitOnThrow(storer::commit);
			}

			@Override
			public void clear()
			{
				storer.clear();
			}

			@Override
			public boolean skipMapped(Object instance, long objectId)
			{
				return storer.skipMapped(instance, objectId);
			}

			@Override
			public boolean skip(Object instance)
			{
				return storer.skip(instance);
			}

			@Override
			public boolean skipNulled(Object instance)
			{
				return storer.skipNulled(instance);
			}

			@Override
			public long size()
			{
				return storer.size();
			}

			@Override
			public long currentCapacity()
			{
				return storer.currentCapacity();
			}

			@Override
			public long maximumCapacity()
			{
				return storer.maximumCapacity();
			}

			@Override
			public Storer reinitialize()
			{
				return storer.reinitialize();
			}

			@Override
			public Storer reinitialize(long initialCapacity)
			{
				return storer.reinitialize(initialCapacity);
			}

			@Override
			public Storer ensureCapacity(long desiredCapacity)
			{
				return storer.ensureCapacity(desiredCapacity);
			}

			@Override
			public void registerCommitListener(PersistenceCommitListener listener)
			{
				storer.registerCommitListener(listener);
			}

			@Override
			public boolean isEmpty()
			{
				return storer.isEmpty();
			}
		}
	}
}
