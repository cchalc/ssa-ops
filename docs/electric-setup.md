# Electric SQL Setup (From Source)

This project uses Electric SQL running from source as an Elixir application.

## Prerequisites

Install [asdf](https://asdf-vm.com) version manager:

```fish
# macOS
brew install asdf
echo -e "\nsource $(brew --prefix asdf)/libexec/asdf.fish" >> ~/.config/fish/config.fish
```

Add required plugins:
```fish
asdf plugin-add elixir
asdf plugin-add erlang
asdf plugin-add nodejs
asdf plugin-add pnpm
```

## Clone Electric SQL

Clone Electric into a sibling directory:
```fish
cd ~/cowork/dev
git clone https://github.com/electric-sql/electric.git
cd electric
asdf install
```

## Configure Environment

Create `packages/sync-service/.env.dev` with Lakebase connection:
```fish
cd packages/sync-service
cat > .env.dev << 'EOF'
DATABASE_URL=postgresql://electric_sync:SyncData2026%21Secure@instance-3a938674-033f-4fcd-8011-656dc6cacbcf.database.cloud.databricks.com:5432/cjc_ssa_ops_dev?sslmode=require
ELECTRIC_INSECURE=true
EOF
```

## Install Dependencies

```fish
cd packages/sync-service
mix deps.get
```

## Run Electric

```fish
mix run --no-halt
```

Electric starts on port 3000 by default.

## Verify

Test the health endpoint:
```fish
curl http://localhost:3000/v1/health
```

Test the shape stream:
```fish
curl "http://localhost:3000/v1/shape?table=test_items"
```

## Run the App

In the ssa-ops directory:
```fish
pnpm dev
```

Navigate to http://localhost:5173/data to see synced data.

## Troubleshooting

### Connection refused
Ensure Lakebase instance is running and native login is enabled.

### Authentication failed
Verify the `electric_sync` user exists in Lakebase:
```fish
databricks psql cjc-ssa-ops-dev --profile fevm-cjc -- -c "\\du electric_sync"
```

### SSL errors
Lakebase requires SSL. Ensure `?sslmode=require` is in DATABASE_URL.
