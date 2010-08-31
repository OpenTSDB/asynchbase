# Copyright 2010 StumbleUpon, Inc.
#
# This library is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library.  If not, see <http://www.gnu.org/licenses/>.

all: jar $(TESTS)
# TODO(tsuna): Use automake to avoid relying on GNU make extensions.

top_builddir = build
package = org.hbase.async
spec_title = Asynchronous HBase
spec_vendor = StumbleUpon, Inc.
spec_version = 1.0
hbaseasync_SOURCES = \
	src/AtomicIncrementRequest.java	\
	src/BrokenMetaException.java	\
	src/Bytes.java	\
	src/ConnectionResetException.java	\
	src/DeleteRequest.java	\
	src/GetRequest.java	\
	src/HBaseClient.java	\
	src/HBaseException.java	\
	src/HBaseRpc.java	\
	src/InvalidResponseException.java	\
	src/KeyValue.java	\
	src/MultiPutRequest.java	\
	src/MultiPutResponse.java	\
	src/NoSuchColumnFamilyException.java	\
	src/NonRecoverableException.java	\
	src/NotServingRegionException.java	\
	src/PutRequest.java	\
	src/RecoverableException.java	\
	src/RegionClient.java	\
	src/RegionInfo.java	\
	src/RegionOfflineException.java	\
	src/RemoteException.java	\
	src/RowLock.java	\
	src/RowLockRequest.java	\
	src/Scanner.java	\
	src/SingletonList.java	\
	src/TableNotFoundException.java	\
	src/UnknownRowLockException.java	\
	src/UnknownScannerException.java	\

hbaseasync_LIBADD = \
	third_party/netty-3.2.2.Final.jar	\
	third_party/slf4j-api-1.6.1.jar	\
	third_party/zookeeper-3.3.1.jar	\
	third_party/suasync-1.0.jar	\

test_SOURCES = src/Test.java
test_LIBADD = \
	$(hbaseasync_LIBADD) \
	third_party/log4j-over-slf4j-1.6.1.jar	\
	third_party/logback-classic-0.9.24.jar	\
	third_party/logback-core-0.9.24.jar	\
        $(jar)

TESTS = $(top_builddir)/Test.class
AM_JAVACFLAGS = -Xlint
JVM_ARGS =
package_dir = $(subst .,/,$(package))
classes=$(hbaseasync_SOURCES:src/%.java=$(top_builddir)/$(package_dir)/%.class)
jar = $(top_builddir)/hbaseasync-$(spec_version).jar
test_classes=$(test_SOURCES:src/%.java=$(top_builddir)/%.class)

jar: $(jar)

get_dep_classpath = `echo $(hbaseasync_LIBADD) | tr ' ' ':'`
$(top_builddir)/.javac-stamp: $(hbaseasync_SOURCES) $(hbaseasync_LIBADD)
	@mkdir -p $(top_builddir)
	javac $(AM_JAVACFLAGS) -cp $(get_dep_classpath) \
	  -d $(top_builddir) $(hbaseasync_SOURCES)
	@touch "$@"

get_runtime_dep_classpath = `echo $(test_LIBADD) | tr ' ' ':'`
$(test_classes): $(jar) $(test_SOURCES) $(test_LIBADD)
	javac $(AM_JAVACFLAGS) -cp $(get_runtime_dep_classpath) \
	  -d $(top_builddir) $(test_SOURCES)

classes_with_nested_classes = $(classes:$(top_builddir)/%.class=%*.class)
test_classes_with_nested_classes = $(test_classes:$(top_builddir)/%.class=%*.class)

check: $(TESTS)
	for i in $(top_builddir)/$(test_classes_with_nested_classes); do \
          case $$i in (*[$$]*) continue;; esac; \
	  java $(JVM_ARGS) -cp $(get_runtime_dep_classpath):$(top_builddir) `basename $${i%.class}` $(ARGS); \
        done

pkg_version = \
  `git rev-list --pretty=format:%h HEAD --max-count=1 | sed 1d || echo unknown`
$(top_builddir)/manifest: $(top_builddir)/.javac-stamp .git/HEAD
	{ echo "Specification-Title: $(spec_title)"; \
          echo "Specification-Version: $(spec_version)"; \
          echo "Specification-Vendor: $(spec_vendor)"; \
          echo "Implementation-Title: $(package)"; \
          echo "Implementation-Version: $(pkg_version)"; \
          echo "Implementation-Vendor: $(spec_vendor)"; } >"$@"

$(jar): $(top_builddir)/manifest $(top_builddir)/.javac-stamp $(classes)
	cd $(top_builddir) && jar cfm `basename $(jar)` manifest $(classes_with_nested_classes) \
         || { rv=$$? && rm -f `basename $(jar)` && exit $$rv; }
#                       ^^^^^^^^^^^^^^^^^^^^^^^
# I've seen cases where `jar' exits with an error but leaves a partially built .jar file!

doc: $(top_builddir)/api/index.html

JDK_JAVADOC=http://download.oracle.com/javase/6/docs/api
NETTY_JAVADOC=http://docs.jboss.org/netty/3.2/api
SUASYNC_JAVADOC=http://tsunanet.net/~tsuna/async/api
JAVADOCS = $(JDK_JAVADOC) $(NETTY_JAVADOC) $(SUASYNC_JAVADOC)
$(top_builddir)/api/index.html: $(hbaseasync_SOURCES)
	javadoc -d $(top_builddir)/api -classpath $(get_dep_classpath) \
          `echo $(JAVADOCS) | sed 's/\([^ ]*\)/-link \1/g'` $(hbaseasync_SOURCES)

clean:
	@rm -f $(top_builddir)/.javac-stamp
	rm -f $(top_builddir)/manifest
	cd $(top_builddir) || exit 0 && rm -f $(classes_with_nested_classes) $(test_classes_with_nested_classes)
	cd $(top_builddir) || exit 0 \
	  && test -d $(package_dir) || exit 0 \
	  && dir=$(package_dir) \
	  && while test x"$$dir" != x"$${dir%/*}"; do \
	       rmdir "$$dir" && dir=$${dir%/*} || break; \
	     done \
	  && rmdir "$$dir"

distclean: clean
	rm -f $(jar)
	rm -rf $(top_builddir)/api
	test ! -d $(top_builddir) || rmdir $(top_builddir)

.PHONY: all jar clean distclean doc check
