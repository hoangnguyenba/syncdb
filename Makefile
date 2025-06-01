build:
	go build -o syncdb cmd/syncdb/*.go

test_export:
	./syncdb export --profile commerce-local-lite --path backup --include-schema=true --include-data=true

test_import:
	./syncdb import --profile commerce-local-test --path backup --include-schema=true --include-data=true --drop

test_export_content:
	./syncdb export --profile content-local-lite --path backup --include-schema=true --include-data=true --zip

test_import_content:
	./syncdb import --profile content-local-test --path backup/contentdbtest_20250601_161500.zip --include-schema=true --include-data=true --drop

test_export_gdrive:
	./syncdb export \
		--profile content-local-lite \
		--storage gdrive \
		--gdrive-folder 15b-n8mPW0Hpp6hGbHShmHHkBvdC7U6d_ \
		--include-schema=true

test_import_gdrive:
	./syncdb import \
		--profile content-local-lite \
		--storage gdrive \
		--gdrive-folder 15b-n8mPW0Hpp6hGbHShmHHkBvdC7U6d_ \
		--include-schema=true