# Copyright (C) 2011-2012  The Async HBase Authors.  All rights reserved.
# This file is part of Async HBase.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#   - Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#   - Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#   - Neither the name of the StumbleUpon nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

SLF4J_VERSION = 1.7.2


LOG4J_OVER_SLF4J_VERSION := $(SLF4J_VERSION)
LOG4J_OVER_SLF4J := third_party/slf4j/log4j-over-slf4j-$(LOG4J_OVER_SLF4J_VERSION).jar
LOG4J_OVER_SLF4J_BASE_URL := $(ASYNCHBASE_THIRD_PARTY_BASE_URL)

$(LOG4J_OVER_SLF4J): $(LOG4J_OVER_SLF4J).md5
	set dummy "$(LOG4J_OVER_SLF4J_BASE_URL)" "$(LOG4J_OVER_SLF4J)"; shift; $(FETCH_DEPENDENCY)


SLF4J_API_VERSION := $(SLF4J_VERSION)
SLF4J_API := third_party/slf4j/slf4j-api-$(SLF4J_API_VERSION).jar
SLF4J_API_BASE_URL := $(ASYNCHBASE_THIRD_PARTY_BASE_URL)

$(SLF4J_API): $(SLF4J_API).md5
	set dummy "$(SLF4J_API_BASE_URL)" "$(SLF4J_API)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(JCL_OVER_SLF4J) $(LOG4J_OVER_SLF4J) $(SLF4J_API)
