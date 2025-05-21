package org.eclipse.datagrid.cluster.nodelibrary.micronaut;

/*-
 * #%L
 * Eclipse DataGrid Cluster Nodelibrary Micronaut
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

import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Primary;
import io.micronaut.core.annotation.Order;
import io.micronaut.eclipsestore.RootProvider;
import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterStorageManager;

@EachBean(ClusterStorageManager.class)
@Primary
@Order(-10)
public class ClusterRootProvider<T> implements RootProvider<T>
{
	private final ClusterStorageManager<T> storageManager;

	public ClusterRootProvider(final ClusterStorageManager<T> storageManager)
	{
		this.storageManager = storageManager;
	}

	@Override
	public T root()
	{
		return this.storageManager.root().get();
	}
}
