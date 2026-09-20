# Hail Money DocuSign integration

## Status

The integration is intentionally isolated from the approved production branch until DocuSign demo credentials are configured and the end-to-end signing flow passes.

## Cost-first rollout

1. Build and test against the free DocuSign Developer demo environment.
2. Do not purchase a production API plan until the signing flow is approved.
3. Before production, compare DocuSign Developer Starter with DocuSign ISV Embed / Pay-as-you-go because Hail Money is a multi-tenant SaaS product.

## DocuSign app configuration

Create one DocuSign integration/app named `Hail Money` in the free developer account.

OAuth redirect URI:

`https://us-central1-hailmoneymap.cloudfunctions.net/docusignOAuthCallback`

OAuth scopes used:

- `signature`
- `extended`

Firebase Secret Manager values required before deploying the DocuSign Functions:

- `DOCUSIGN_CLIENT_ID` - DocuSign Integration Key / Client ID
- `DOCUSIGN_CLIENT_SECRET` - DocuSign secret key
- `DOCUSIGN_TOKEN_KEY` - Hail Money-generated 32-byte encryption key, base64 or 64-character hex

`DOCUSIGN_ENVIRONMENT` defaults to `demo`. Set it to `production` only after DocuSign Go-Live and production credentials are ready.

## Hail Money functions

- `docusignConnect` - owner/admin starts OAuth connection for the company
- `docusignOAuthCallback` - securely completes OAuth and stores encrypted tokens
- `docusignStatus` - checks whether the company is connected
- `docusignDisconnect` - owner/admin removes the company connection
- `docusignSendEnvelope` - sends a Hail Money PDF for signature
- `docusignEnvelopeStatus` - loads and records current envelope status
- `docusignRecipientView` - creates an embedded signing URL for in-person/in-app signing

Each connection is scoped by Hail Money `hmOrganizationId`, so one roofing company's DocuSign account cannot be used by another company.

## Signature placement

Hail Money PDFs can provide explicit signature coordinates or signature anchors. If no explicit placement is supplied, signer 1 uses:

`[[DOCUSIGN_SIGNATURE_1]]`

Signer 2 uses:

`[[DOCUSIGN_SIGNATURE_2]]`

and so on. The API deliberately refuses to silently ignore a missing signature anchor, preventing an agreement from being sent with no required signature field.

## Security

- DocuSign client secret never enters browser code.
- OAuth access and refresh tokens are encrypted with AES-256-GCM before Firestore storage.
- The encryption key is stored as a Firebase secret, not in GitHub.
- OAuth state records expire after 10 minutes.
- Company isolation uses the existing Hail Money organization claim.
- Return URLs are restricted to Hail Money hosts and local development.
