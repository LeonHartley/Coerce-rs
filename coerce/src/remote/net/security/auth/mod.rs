use crate::remote::net::security::jwt::Jwt;

#[cfg(feature = "client-auth-jwt")]
pub mod jwt;

pub enum ClientAuth {
    None,

    #[cfg(feature = "client-auth-jwt")]
    Jwt(Jwt),
}

impl Default for ClientAuth {
    fn default() -> Self {
        Self::None
    }
}

impl ClientAuth {
    pub fn generate_token(&self) -> String {
        match &self {
            ClientAuth::None => String::default(),

            #[cfg(feature = "client-auth-jwt")]
            ClientAuth::Jwt(jwt) => jwt.generate_token().unwrap(),
        }
    }

    pub fn validate_token(&self, token: &str) -> bool {
        match &self {
            ClientAuth::None => true,

            #[cfg(feature = "client-auth-jwt")]
            ClientAuth::Jwt(jwt) => jwt.validate_token(token),
        }
    }
}
