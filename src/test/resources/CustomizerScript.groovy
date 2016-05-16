/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2016 ForgeRock AS. All Rights Reserved
 *
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the License). You may not use this file except in
 * compliance with the License.
 *
 * You can obtain a copy of the License at
 * http://forgerock.org/license/CDDLv1.0.html
 * See the License for the specific language governing
 * permission and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL
 * Header Notice in each file and include the License file
 * at http://forgerock.org/license/CDDLv1.0.html
 * If applicable, add the following below the CDDL Header,
 * with the fields enclosed by brackets [] replaced by
 * your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 */


import com.datastax.driver.core.Cluster
import com.datastax.driver.core.PoolingOptions
import org.forgerock.openicf.connectors.cassandra.CassandraConfiguration
import org.identityconnectors.common.security.GuardedString
import org.identityconnectors.common.security.SecurityUtil

customize {
    init { Cluster.Builder builder ->

        def c = delegate as CassandraConfiguration

        c.getContactPoints().each { cp -> builder.addContactPoint(cp) }

        builder.withCredentials(c.getUsername(), SecurityUtil.decrypt(c.getPassword()))


        def PoolingOptions poolingOptions = new PoolingOptions();

        //builder.withPoolingOptions(poolingOptions)


        builder.withoutJMXReporting().withoutMetrics()
    }

    /**
     * This Closure can release allocated resource.
     */
    release {
    }

}