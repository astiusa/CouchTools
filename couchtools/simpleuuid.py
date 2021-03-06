# Copyright 2012, Advanced Simulation Technology, inc. http://www.asti-usa.com/
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


class simpleuuid(object):
    '''
    In case you're running on a machine so old it doesn't have a uuid module, eg python 2.4
    Also note that this is not even remotely RFC4122-compliant, it's an emergency fallback.
    '''
    def __init__(self):
        import random
        self.hex = ''
        for x in range(1, 32):
            self.hex += random.choice('abcde0123456789')


def uuid():
    return simpleuuid()
