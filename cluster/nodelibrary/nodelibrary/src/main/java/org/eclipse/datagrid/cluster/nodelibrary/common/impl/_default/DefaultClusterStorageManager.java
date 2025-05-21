package org.eclipse.datagrid.cluster.nodelibrary.common.impl._default;

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

import org.eclipse.serializer.afs.types.ADirectory;

import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterStorageManager;

public interface DefaultClusterStorageManager<T> extends ClusterStorageManager<T>
{
	@Override
	default void issueFullBackup(final ADirectory path)
	{
		throw new UnsupportedOperationException("Only micro storages support this feature!");
	}

	abstract class Abstract<T> extends ClusterStorageManager.Abstract<T> implements DefaultClusterStorageManager<T>
	{
		@Override
		public boolean isReady()
		{
			// Always true after initialization
			return true;
		}
	}
}
