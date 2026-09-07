# IAM for the nightly feed-quality audit

The audit workflow assumes one purpose-built, read-mostly role through GitHub
OIDC. No long-lived AWS keys exist for this repository.

| File | Purpose |
|---|---|
| `favi-feed-audit-trust.json` | Trust policy: only GitHub Actions runs of **this repository's `main` branch** may assume the role (`token.actions.githubusercontent.com:sub` is pinned to `repo:98RaulB/feed_checker_gui:ref:refs/heads/main`). |
| `favi-feed-audit-permissions.json` | Permissions: `dynamodb:Scan` / `DescribeTable` on the `feed_configs` table (to list feeds) and `s3:PutObject` on the private `_meta/audit/*` prefix (to publish the report). Nothing else. |

`<ACCOUNT_ID>` is a placeholder. The real account id is deployment
configuration, not source: the role's ARN is stored in the repository secret
`AUDIT_ROLE_ARN`, which the workflow reads.

## Provisioning

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
sed "s/<ACCOUNT_ID>/$ACCOUNT_ID/g" favi-feed-audit-trust.json > /tmp/trust.json
sed "s/<ACCOUNT_ID>/$ACCOUNT_ID/g" favi-feed-audit-permissions.json > /tmp/perms.json

aws iam create-role --role-name favi-feed-audit \
  --assume-role-policy-document file:///tmp/trust.json
aws iam put-role-policy --role-name favi-feed-audit \
  --policy-name favi-feed-audit --policy-document file:///tmp/perms.json

gh secret set AUDIT_ROLE_ARN --body "arn:aws:iam::$ACCOUNT_ID:role/favi-feed-audit"
```

The GitHub OIDC provider (`token.actions.githubusercontent.com`) must already
exist in the account; the pipeline repository owns it.
