[![](https://jitci.com/gh/Sylvyrfysh/kotlin-lmdb-wrapper/svg)](https://jitci.com/gh/Sylvyrfysh/kotlin-lmdb-wrapper)
[![](https://jitpack.io/v/com.nicholaspjohnson/kotlin-lmdb-wrapper.svg)](https://jitpack.io/#com.nicholaspjohnson/kotlin-lmdb-wrapper)

# kotlin-lmdb-wrapper

## What is this?

This project is meant to provide a wrapper around an in-process Key-Value DB that is extremely fast.
This lead me to choose LMDB as the underlying DB, which is accessed through LWJGL.
I designed it as a final project for my Database Systems class, and have grown it so that it is more useful since.

## How do I use it?

coming soon...

## My classes contain different data since I last wrote them. How do I migrate them?

coming soon...

## Future feature goals

&#x274c; Targeted for this version

&#x2714; Implemented in this version

|                            | 0.2.0    | 0.3.0    |
|:-------------------------- |:-------- | -------- |
| Better Key Buffer          | &#x2714; |          |
| Refactor RWPs              | &#x2714; |          |
| Nullable Properties        | &#x2714; |          |
| Allow Value Initialization | &#x2714; |          |
| Custom RWPs                | &#x2714; |          |
| List RWP                   | &#x2714; |          |
| Map RWP                    | &#x2714; |          |
| Type-Wrapped RWP           | &#x2714; |          |
| Class Version Migration    |          | &#x274c; |
| ThreadLocal R/W TXes       |          | &#x274c; |
| Map/List constructor call  |          | &#x274c; |
| Default keys               |          | &#x274c; |
| Class is a DBI             |          | &#x274c; |
