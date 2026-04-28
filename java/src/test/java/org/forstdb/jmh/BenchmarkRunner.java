/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.forstdb.jmh;

import java.util.ArrayList;
import java.util.List;

import org.openjdk.jmh.Main;

/**
 * Thin wrapper that lets {@code mvn -Pbench exec:java} drive the JMH
 * harness. Forwards CLI args straight to JMH so filtering ({@code -p},
 * {@code -wi}, regex matching, etc.) works as documented at
 * <a href="https://github.com/openjdk/jmh">openjdk/jmh</a>.
 *
 * <p>Defaults applied if the user did not specify them:
 * <ul>
 *   <li>{@code -f 0} — run in-process. JMH normally forks a fresh JVM
 *       per benchmark, but {@code exec:java}'s classpath is not
 *       inherited by the fork (you'd see
 *       {@code ClassNotFoundException: org.openjdk.jmh.runner.ForkedMain}).
 *       For real fork-isolated measurements, run the benchmarks JAR
 *       directly with {@code java -jar} after building it via
 *       {@code bench.sh}.</li>
 *   <li>The {@code org.forstdb.jmh} package as the include filter.</li>
 * </ul>
 *
 * <pre>
 *   mvn -Pbench test-compile exec:java
 *   mvn -Pbench test-compile exec:java -Dexec.args="get_hit"
 * </pre>
 */
public final class BenchmarkRunner {
    private BenchmarkRunner() {}

    public static void main(final String[] args) throws Exception {
        final List<String> finalArgs = new ArrayList<>(args.length + 4);
        boolean hasFork = false;
        boolean hasFilter = false;
        for (int i = 0; i < args.length; i++) {
            finalArgs.add(args[i]);
            if ("-f".equals(args[i])) {
                hasFork = true;
            } else if (!args[i].startsWith("-")) {
                hasFilter = true;
            }
        }
        if (!hasFork) {
            finalArgs.add("-f");
            finalArgs.add("0");
        }
        if (!hasFilter) {
            finalArgs.add("org.forstdb.jmh");
        }
        Main.main(finalArgs.toArray(new String[0]));
    }
}
