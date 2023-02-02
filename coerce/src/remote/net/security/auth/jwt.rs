use chrono::{DateTime, TimeZone, Utc};
use hmac::digest::KeyInit;
use hmac::Hmac;
use jwt::{Error, SignWithKey, VerifyWithKey};
use sha2::Sha256;

use std::time::Duration;
use uuid::Uuid;

pub struct Jwt {
    secret: Hmac<Sha256>,
    token_ttl: Option<Duration>,
}

#[derive(Serialize, Deserialize)]
struct Payload {
    id: Uuid,
    timestamp: DateTime<Utc>,
}

impl Jwt {
    pub fn from_secret<S: Into<Vec<u8>>>(secret: S, token_ttl: Option<Duration>) -> Jwt {
        let secret = secret.into();
        let secret: Hmac<Sha256> = Hmac::new_from_slice(&secret)
            .map_err(|_e| "invalid client auth secret key")
            .unwrap();

        Self { secret, token_ttl }
    }

    pub fn generate_token(&self) -> Option<String> {
        let payload = Payload {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
        };

        SignWithKey::sign_with_key(payload, &self.secret).ok()
    }

    pub fn validate_token(&self, token: &str) -> bool {
        let payload: Result<Payload, Error> = VerifyWithKey::verify_with_key(token, &self.secret);

        if let Ok(payload) = payload {
            if let Some(token_ttl) = &self.token_ttl {
                is_token_still_valid(&payload, token_ttl)
            } else {
                true
            }
        } else {
            false
        }
    }
}

fn is_token_still_valid(payload: &Payload, token_ttl: &Duration) -> bool {
    let token_ttl = chrono::Duration::from_std(*token_ttl).unwrap();
    let token_expires_at = payload.timestamp + token_ttl;
    let now = Utc::now();

    if now <= token_expires_at {
        true
    } else {
        false
    }
}
