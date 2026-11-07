#pragma once

#include "public.h"

namespace NYT::NHydraStressTest {

////////////////////////////////////////////////////////////////////////////////

//! Checks that an acknowledged mutation survives when a follower restarts and
//! rejoins the same epoch while it holds records past the leader's applied state.
//!
//! 5 voting peers: leader L, followers A, B, C, D.
//!  1. AcceptMutations from L to C and D fail; those from L to B are held.
//!  2. A CAS mutation M is logged by L and A only.
//!  3. A restarts in place and rejoins the same epoch; after its initial ping,
//!     AcceptMutations from L to A fail.
//!  4. B's held requests are released; M is acknowledged to the client.
//!  5. L and B are stopped (2 of 5 peers); A, C and D elect a new leader.
//! Reading from the new leader must return the value written by M.
//! Returns 0 if it does, 1 if the acknowledged write is lost, 2 if the run is inconclusive.
int RunFollowerRejoinScenario(const TString& workDir);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
