#!groovy

import jenkins.model.*
import hudson.security.*
import jenkins.install.InstallState

def instance = Jenkins.getInstance()

// Create user with custom pass
def hudsonRealm = new HudsonPrivateSecurityRealm(false)
hudsonRealm.createAccount('admin','jenadmin')
instance.setSecurityRealm(hudsonRealm)

def strategy = new FullControlOnceLoggedInAuthorizationStrategy()
strategy.setAllowAnonymousRead(false)
instance.setAuthorizationStrategy(strategy)

// Disable setup wizard
if (!instance.installState.isSetupComplete()) {
  InstallState.INITIAL_SETUP_COMPLETED.initializeState()
}

instance.save()

