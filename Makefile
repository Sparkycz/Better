.PHONY: develop test docs pip-update clean update-dependencies

build: .venv submodules-update

develop:
	docker-compose up api

test:
	docker-compose up --abort-on-container-exit elasticsearch redis mysql test_api

update-dependencies: submodules-update pip-update

submodules-update:
	git submodule update --init --rebase --remote

pip-update:
	.venv/bin/pip install --upgrade pip-tools
	.venv/bin/pip-compile --upgrade requirements.in
	.venv/bin/pip install -r requirements.txt
	@echo "Requirements updated. BEWARE that package downgrade/removal will take effect after venv re-creation."


clean:
	rm -rf .venv
	find . -iname "*.pyc" -exec rm {} \;


.venv:
	"$(shell which python3)" -m venv .venv
	.venv/bin/pip install --upgrade pip setuptools  # because of Jenkins
	.venv/bin/pip install -r requirements.txt

deploy:
	docker build . -t registry.gitlab.heu.cz/catalogue/search/indexer:debug
	docker push registry.gitlab.heu.cz/catalogue/search/indexer:debug
	helm delete search-indexer-cz --tiller-namespace team-purple --purge
	sleep 1
	helm upgrade --install "search-indexer-cz" chart/ --namespace=search --tiller-namespace=team-purple
