module kotlin.lmdb.wrapper.main {
    requires java.base;
    requires jdk.unsupported;
    requires kotlin.stdlib;
    requires kotlin.stdlib.jdk7;
    requires kotlinx.serialization.core.jvm;
    requires org.lwjgl;
    requires org.lwjgl.lmdb;

    exports com.nicholaspjohnson.kotlinlmdbwrapper;
    exports com.nicholaspjohnson.kotlinlmdbwrapper.lmdb;
    exports com.nicholaspjohnson.kotlinlmdbwrapper.serializers;
    exports com.nicholaspjohnson.kotlinlmdbwrapper.serializestrategies;
}