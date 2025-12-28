# FormiDB

## About

A **very** lightweight database engine implemented from scratch. (Don't expect a lot of features lol, it's just a proof of concept)

---

## Build

Uses CMake for builds.

### Creating a build directory (Recommended)

From the project root:

```bash
mkdir build
cd build
```

### Configuring and building

Under the `build` directory

```bash
cmake ..
cmake --build .
```

After a successful build, two binaries should be generates as of now, one is the main application (formidb) and the other is for tests (only one as of now ;-;)

---

## Debug Builds, Testing, and Tooling

### Building with CMake Debug configuration

To build FormiDB with debug symbols enabled:

```bash
mkdir build-debug
cd build-debug
cmake -DCMAKE_BUILD_TYPE=Debug ..
cmake --build .
```

This configuration enables symbols required for debugging and memory analysis tools.

---

### Debugging with gdb

Run the built executable under `gdb`:

```bash
gdb ./formidb
```
---

### Memory checking with Valgrind

Best tool in existence fr

```bash
valgrind --leak-check=full --track-origins=yes ./formidb
```
---

### In development
1. Minimal SQL like interpreter to run in shell mode
2. Thread safe pager
