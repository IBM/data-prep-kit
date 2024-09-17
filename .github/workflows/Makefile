
UNIVERSAL_TRANSFORMS=noop doc_id

transform-tests:
	for i in $(UNIVERSAL_TRANSFORMS); do	\
	    cat test-transform.template | sed -e "s?@TARGET_TRANSFORM_DIR@?transforms/universal/$$i?g" > test-universal-$$i.yml;	\
	done

