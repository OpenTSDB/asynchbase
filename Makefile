SITE_HTML = \
  index.html	\
  faq.html	\

CONTENT_DIR = content
CONTENT_HEADER = $(CONTENT_DIR)/header
CONTENT_FOOTER = $(CONTENT_DIR)/footer

all: $(SITE_HTML)

%.html: $(CONTENT_DIR)/%.content
	sed '/^<!--.*-->$$/d' $< | cat $(CONTENT_HEADER) - $(CONTENT_FOOTER) > $@-t
	title=`sed -n 's/<!--title: *\([^>]*\) *-->/\1 - /p' $<` && \
	  sed "s/<title>/&$$title/" $@-t > $@
	rm -f $@-t

$(SITE_HTML): $(CONTENT_HEADER) $(CONTENT_FOOTER)

clean:
	rm -f $(SITE_HTML:.html=.html-t)

distclean: clean
	rm -f $(SITE_HTML)

.PHONY: all clean distclean
