module kotlin.lmdb.wrapper.main {
    requires kotlin.stdlib;
    requires kotlinx.serialization.core.jvm;
    requires org.lwjgl;
    requires org.lwjgl.lmdb;
    requires kotlin.stdlib.jdk7;

    exports com.nicholaspjohnson.kotlinlmdbwrapper;
    exports com.nicholaspjohnson.kotlinlmdbwrapper.lmdb;
    exports com.nicholaspjohnson.kotlinlmdbwrapper.serializers;
    exports com.nicholaspjohnson.kotlinlmdbwrapper.serializestrategies;
}