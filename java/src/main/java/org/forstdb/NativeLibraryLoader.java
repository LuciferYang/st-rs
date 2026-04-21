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

package org.forstdb;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * Loads the st-rs native library ({@code libst_rs_jni.so} on Linux,
 * {@code libst_rs_jni.dylib} on macOS).
 *
 * <p>Load order:
 * <ol>
 *   <li>Try {@code System.loadLibrary("st_rs_jni")} — works if the
 *       library is on {@code java.library.path}.</li>
 *   <li>Try extracting from the JAR at
 *       {@code /org/forstdb/native/&lt;os&gt;-&lt;arch&gt;/&lt;libname&gt;} to a temp
 *       file and loading it via {@code System.load()}.</li>
 * </ol>
 *
 * <p>To bundle the native library inside the JAR, place it at:
 * <pre>
 *   src/main/resources/org/forstdb/native/linux-x86_64/libst_rs_jni.so
 *   src/main/resources/org/forstdb/native/osx-aarch64/libst_rs_jni.dylib
 * </pre>
 */
public class NativeLibraryLoader {

    private static final String LIB_NAME = "st_rs_jni";
    private static volatile boolean loaded = false;
    private static final NativeLibraryLoader INSTANCE = new NativeLibraryLoader();

    /**
     * Singleton accessor for compatibility with Flink's
     * {@code ForStStateBackend}, which retrieves the loader as
     * {@code NativeLibraryLoader.getInstance().loadLibrary(tmpDir)}.
     */
    public static NativeLibraryLoader getInstance() {
        return INSTANCE;
    }

    /**
     * Compatibility shim for Flink's {@code ForStStateBackend}, which calls
     * this with a temporary directory hint for native lib extraction. We
     * ignore the hint and defer to {@link #load()}'s standard strategy
     * (java.library.path, then JAR extraction to the system temp dir).
     */
    public synchronized void loadLibrary(final String tmpDir) throws IOException {
        load();
    }

    /**
     * Load the native library. Safe to call multiple times.
     */
    public static synchronized void load() {
        if (loaded) {
            return;
        }

        // 1. Try java.library.path first.
        try {
            System.loadLibrary(LIB_NAME);
            loaded = true;
            return;
        } catch (final UnsatisfiedLinkError ignored) {
            // Fall through to JAR extraction.
        }

        // 2. Try extracting from inside the JAR.
        final String resourcePath = nativeResourcePath();
        try (InputStream in = NativeLibraryLoader.class
                .getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new UnsatisfiedLinkError(
                        "Native library not found on java.library.path "
                        + "and not bundled in JAR at " + resourcePath);
            }

            final File tempFile = File.createTempFile("st_rs_jni", libExtension());
            tempFile.deleteOnExit();
            Files.copy(in, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            System.load(tempFile.getAbsolutePath());
            loaded = true;
        } catch (final IOException e) {
            throw new UnsatisfiedLinkError(
                    "Failed to extract native library from JAR: " + e.getMessage());
        }
    }

    /**
     * Resource path inside the JAR for the current platform.
     */
    static String nativeResourcePath() {
        return "/org/forstdb/native/" + osPlatform() + "/" + libFileName();
    }

    private static String libFileName() {
        return System.mapLibraryName(LIB_NAME);
    }

    private static String libExtension() {
        final String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("mac") || os.contains("darwin")) {
            return ".dylib";
        }
        return ".so";
    }

    private static String osPlatform() {
        final String os = System.getProperty("os.name", "").toLowerCase();
        final String arch = System.getProperty("os.arch", "").toLowerCase();

        final String osName;
        if (os.contains("linux")) {
            osName = "linux";
        } else if (os.contains("mac") || os.contains("darwin")) {
            osName = "osx";
        } else if (os.contains("win")) {
            osName = "windows";
        } else {
            osName = os.replaceAll("\\s+", "_");
        }

        final String archName;
        if ("amd64".equals(arch) || "x86_64".equals(arch)) {
            archName = "x86_64";
        } else if ("aarch64".equals(arch) || "arm64".equals(arch)) {
            archName = "aarch64";
        } else {
            archName = arch;
        }

        return osName + "-" + archName;
    }
}
