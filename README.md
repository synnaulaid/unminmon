# unminmon

## Payout progress

The Unmineable assets API does not include each coin's minimum payout threshold. LTC defaults to the official 0.00075 LTC threshold for its default network. If your payout network uses a different threshold, override it in `.env`:

```dotenv
PAYOUT_MINIMUMS=LTC:<minimum>,DOGE:<minimum>
```

Replace each placeholder with that coin's actual minimum payout amount, then restart the monitor. The dashboard shows progress to the threshold and marks payout complete when the balance drops after reaching 100%.