To setup run: `ESB_FEATURES=umadb,axonserver make build` and `./aws/durability/00-setup-ssh-key.sh`.

To run the durability test either `./aws/durability/orchestrate-durability-test.sh --store umadb` or `./aws/durability/orchestrate-durability-test.sh --store axonserver`.