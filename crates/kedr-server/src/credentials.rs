// SPDX-License-Identifier: Apache-2.0
//! SCRAM verifiers only. Neither plaintext nor SaltedPassword is persisted.
use ring::rand::{SecureRandom, SystemRandom};
use rsasl::{
    callback::{Context, Request, SessionCallback, SessionData},
    mechanisms::scram::{properties::ScramStoredPassword, tools},
    prelude::{SASLConfig, SASLServer, SessionError},
    property::{AuthId, AuthzId},
    validate::{Validate, Validation, ValidationError},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    path::Path,
};
use zeroize::Zeroize;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    version: u32,
    pub(crate) username: String,
    iterations: u32,
    salt: [u8; 16],
    stored_key: [u8; 32],
    server_key: [u8; 32],
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl Credential {
    pub fn new(username: &str, password: &str) -> io::Result<Self> {
        if username.is_empty() || username.len() > 63 || username.contains('\0') {
            return Err(invalid("username must be 1..63 UTF-8 bytes without NUL"));
        }
        if !(8..=1024).contains(&password.len()) || password.contains('\0') {
            return Err(invalid("password must be 8..1024 UTF-8 bytes without NUL"));
        }
        let normalized = zeroize::Zeroizing::new(
            stringprep::saslprep(password)
                .map_err(|_| invalid("password is not valid SASLprep text"))?
                .into_owned(),
        );
        let mut salt = [0; 16];
        SystemRandom::new()
            .fill(&mut salt)
            .map_err(|_| invalid("OS random source failed"))?;
        let mut salted = Default::default();
        tools::hash_password::<Sha256>(normalized.as_bytes(), 16384, &salt, &mut salted);
        let (mut client, server) = tools::derive_keys::<Sha256>(&salted[..]);
        let stored_key = Sha256::digest(client).into();
        client[..].zeroize();
        salted[..].zeroize();
        Ok(Self {
            version: 1,
            username: username.into(),
            iterations: 16384,
            salt,
            stored_key,
            server_key: server.into(),
        })
    }
    pub fn write_new(&self, path: &Path) -> io::Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path)?;
        let bytes = serde_json::to_vec(self).map_err(|_| invalid("cannot encode credential"))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    }
    pub fn read(path: &Path) -> io::Result<Self> {
        let meta = fs::symlink_metadata(path)?;
        if !meta.is_file() || meta.len() > 8192 {
            return Err(invalid(
                "credential must be a regular file smaller than 8 KiB",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if meta.permissions().mode() & 0o077 != 0 {
                return Err(invalid(
                    "credential file must not be accessible by group or others",
                ));
            }
        }
        let file = fs::File::open(path)?;
        let opened = file.metadata()?;
        if !opened.is_file() || opened.len() > 8192 {
            return Err(invalid("invalid opened credential file"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            if meta.dev() != opened.dev()
                || meta.ino() != opened.ino()
                || opened.permissions().mode() & 0o077 != 0
            {
                return Err(invalid("credential file changed or has unsafe permissions"));
            }
        }
        let mut bytes = Vec::new();
        file.take(8193).read_to_end(&mut bytes)?;
        if bytes.len() > 8192 {
            return Err(invalid("credential size limit"));
        }
        let credential: Self =
            serde_json::from_slice(&bytes).map_err(|_| invalid("invalid credential file"))?;
        if credential.version != 1
            || credential.iterations < 4096
            || credential.iterations > 1_000_000
            || credential.username.is_empty()
            || credential.username.len() > 63
            || credential.username.contains('\0')
        {
            return Err(invalid("unsupported credential version or values"));
        }
        Ok(credential)
    }
    pub(crate) fn server(&self) -> io::Result<SASLServer<Authenticated>> {
        let config = SASLConfig::builder()
            .with_defaults()
            .with_callback(self.clone())
            .map_err(|_| invalid("cannot configure SCRAM"))?;
        Ok(SASLServer::new(config))
    }
}
pub(crate) struct Authenticated;
impl Validation for Authenticated {
    type Value = bool;
}
impl SessionCallback for Credential {
    fn callback(
        &self,
        _: &SessionData,
        context: &Context,
        request: &mut Request,
    ) -> Result<(), SessionError> {
        // PostgreSQL authenticates Startup.user; SCRAM's n= may be empty in libpq.
        if context
            .get_ref::<AuthId>()
            .is_some_and(|id| id.is_empty() || id == self.username)
        {
            request.satisfy::<ScramStoredPassword>(&ScramStoredPassword::new(
                self.iterations,
                &self.salt,
                &self.stored_key,
                &self.server_key,
            ))?;
        }
        Ok(())
    }
    fn validate(
        &self,
        _: &SessionData,
        context: &Context,
        validate: &mut Validate<'_>,
    ) -> Result<(), ValidationError> {
        // Called by rsasl only after the cryptographic proof matches. Finished alone is insufficient.
        let authid = context.get_ref::<AuthId>();
        let authzid = context.get_ref::<AuthzId>();
        let allowed = authid.is_some_and(|id| id.is_empty() || id == self.username)
            && authzid.is_none_or(|id| id.is_empty() || id == self.username);
        validate.finalize::<Authenticated>(allowed);
        Ok(())
    }
}

pub(crate) fn random<const N: usize>() -> io::Result<[u8; N]> {
    let mut bytes = [0; N];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| invalid("OS random source failed"))?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsasl::{mechname::Mechname, prelude::SASLClient};
    #[test]
    fn scram_requires_proof_and_validates_libpq_empty_authid() {
        let credential = Credential::new("admin", "correct-password").unwrap();
        for (name, password, accepted) in [
            ("admin", "correct-password", true),
            ("admin", "wrong-password", false),
            ("other", "correct-password", false),
        ] {
            let config = SASLConfig::with_credentials(None, name.into(), password.into()).unwrap();
            let client = SASLClient::new(config);
            let mechanism = Mechname::parse(b"SCRAM-SHA-256").unwrap();
            let mut client = client.start_suggested(&[mechanism]).unwrap();
            let server = credential.server().unwrap();
            let mut server = server.start_suggested(mechanism).unwrap();
            let mut first = Vec::new();
            client.step(None, &mut first).unwrap();
            let mut challenge = Vec::new();
            if server.step(Some(&first), &mut challenge).is_err() {
                assert!(!accepted);
                continue;
            }
            let mut proof = Vec::new();
            client.step(Some(&challenge), &mut proof).unwrap();
            let mut final_message = Vec::new();
            let result = server.step(Some(&proof), &mut final_message);
            assert_eq!(
                result.is_ok() && server.validation() == Some(true),
                accepted
            );
        }
        let encoded = serde_json::to_string(&credential).unwrap();
        assert!(!encoded.contains("correct-password"));
        let restored: Credential = serde_json::from_str(&encoded).unwrap();
        assert_eq!(restored.stored_key, credential.stored_key);
    }
}
