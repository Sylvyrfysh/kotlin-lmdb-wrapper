[![JitCI Build](https://jitci.com/gh/Sylvyrfysh/kotlin-lmdb-wrapper/svg)](https://jitci.com/gh/Sylvyrfysh/kotlin-lmdb-wrapper)
[![JitPack Artifact version](https://jitpack.io/v/com.nicholaspjohnson/kotlin-lmdb-wrapper.svg)](https://jitpack.io/#com.nicholaspjohnson/kotlin-lmdb-wrapper)

# KLMDB (kotlin-lmdb-wrapper)

## What is this?

This project is meant to provide a wrapper around an in-process Key-Value DB that is extremely fast.
This lead me to choose [LMDB](https://symas.com/lmdb/) as the underlying DB, which is accessed through [LWJGL](https://github.com/LWJGL/lwjgl3).
I designed it as a final project for my Database Systems class, and have grown it so that it is more useful since.

KLMDB is an effort to make writing Kotlin classes into an LMDB database much easier.
It does this by creating an object-oriented wrapper aound the LMDB environment, databases, and data objects.
The data objects are serialized using the kotlinx-serialization library, meaning that no custom serialization code is typically needed.
It adds simplicity to database operations as well, offering a multitude of operations to avoid using the core LMDB code yourself.
This is exposed as a sequence API, which leverages the core Kotlin featureset.

## How do I use it?

Samples can be found in the **TODO** directory.

## How can I get it?

Builds are offered through [JitPack](https://jitpack.io/#sylvyrfysh/kotlin-lmdb-wrapper).
You'll need to add a top level `repository` block, with this line:
```kotlin
repositories {
  maven { url = URI("https://jitpack.io") }
}
```
And this dependency:
```kotlin
dependencies {
  ...
  implementation("com.nicholaspjohnson", "kotlin-lmdb-wrapper", "0.4.0")
}
```

For more documentation, check out the [documentation](docs) folder.
